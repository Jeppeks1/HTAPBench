/*
 * Copyright 2017 by INESC TEC
 * Developed by Fábio Coelho
 * This work was based on the OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pt.haslab.htapbench.core;

import pt.haslab.htapbench.benchmark.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements the ClientBalacer which is responsible for deciding if more OLAP streams should be added to system.
 * The decision is bounded to a configurable error margin. We accept to degrade up to ERROR_MARGIN(%) of the OLTP
 * targetTPS in trade to add another OLAP Stream.
 */
public class ClientBalancer implements Runnable {

    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(ClientBalancer.class);

    private HTAPBenchmark bench;

    private ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
    private LatencyRecord latencies;
    private List<Worker> workers;
    private Workload workload;
    private Results results;

    // Sampling rate;
    private final int deltaT = 60;
    private double error_margin;

    private volatile boolean processedResults = false;
    private volatile boolean terminate = false;
    private boolean saturated = false;
    private double integral = 0;
    private int olapStreams = 0;
    private int projectedThroughput;
    private int throughput = 0;
    private int timeInterval = 0;

    ClientBalancer(HTAPBenchmark bench, List<Worker> workers, Workload workload) {
        WorkloadConfiguration wrkld = bench.getWorkloadConfiguration();

        this.latencies = new LatencyRecord(0);
        this.projectedThroughput = wrkld.getTargetTPS() * deltaT;
        this.error_margin = wrkld.getErrorMargin();

        this.workload = workload;
        this.workers = workers;
        this.bench = bench;
    }

    /**
     * Computes the current throughput count by reading all workers stats.
     */
    private void getThroughput() {
        int txn_count = 0;
        for (Worker w : workers) {
            if (w instanceof TPCCWorker)
                txn_count += w.getTxncount();
        }

        // Set the observed throughput
        throughput = txn_count;

        // Record the TPS for a given number of active OLAP streams
        latencies.addLatency(olapStreams, timeInterval, throughput, deltaT);

        // Update the current time interval
        timeInterval += deltaT;
    }

    @Override
    public void run() {
        long measureStart = System.nanoTime();
        int requests = 0;

        try {
            while (!terminate) {
                requests++;
                Thread.sleep(deltaT * 1000);

                // Reads and computes the current observed throughput.
                getThroughput();

                double error = projectedThroughput - throughput;
                integral = integral + error / deltaT;

                double ki = 0.03;
                double kp = 0.4;
                double output = kp * error + ki * integral;

                /*
                 * Take decision. If the the total targetTPS is within the
                 * error margin --> Launch another OLAP Stream.
                 */
                LOG.info("TPS: " + throughput / deltaT);
                LOG.info("output: " + output);
                LOG.info("error: " + error);

                if (olapStreams == 0 || (!saturated && output < error_margin * projectedThroughput)) {
                    workload.addTPCHWorker();
                    LOG.info("ClientBalancer: Going to lauch 1 OLAP stream. Total OLAP Streams: " + (++olapStreams));
                } else {
                    saturated = true;
                    LOG.info("***************************************************************************************************");
                    LOG.info("         ClientBalancer: The system is saturated. No more OLAP streams will be lauched.");
                    LOG.info("***************************************************************************************************");
                }

                LOG.info("***************************************************************************************************");
                LOG.info("                          #ACTIVE OLAP STREAMS: " + olapStreams);
                LOG.info("***************************************************************************************************");
            }
        } catch (InterruptedException ex) {
            if (!terminate)
                LOG.warn("ClientBalancer received an unexpected InterruptedException.");

            int samplingSize = bench.getWorkloadConfiguration().getSamplingSize();
            int duration = bench.getWorkloadConfiguration().getBenchmarkDuration();

            // The ClientBalancer might be interrupted shortly before it was due to record the final
            // throughput. Guard for this case by examining the expected number  records in the latency
            // list. Integer division intended, so that the value is rounded down.
            if (latencies.size() != duration / samplingSize)
                getThroughput();
        }

        long measureEnd = System.nanoTime();

        for (LatencyRecord.Sample sample : getLatencyRecords()) {
            if (sample != null)
                samples.add(sample);
        }

        Collections.sort(samples);

        // Compute stats on all the latencies
        long[] latencies = new long[samples.size()];
        for (int i = 0; i < samples.size(); ++i) {
            latencies[i] = samples.get(i).getLatencyUs();
        }
        DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

        results = new Results(measureEnd - measureStart, requests, stats, samples);
        results.setName("ClientBalancer");

        processedResults = true;

        LOG.info("[ClientBalancer] Finished collecting results ..");
    }

    Results getResults(boolean isHybrid) {
        // There are no results to be had if the workload is not Hybrid
        if (!isHybrid)
            return null;

        // Allow the interrupted thread some time to process the results
        while (!processedResults) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return results;
    }

    /**
     * Sets the terminate flag before calling the interrupt method of
     * the ClientBalancer thread. This is used to indicate if its an
     * expected shutdown or not.
     */
    void terminate(){
        terminate = true;
    }

    private Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return this.latencies;
    }
}
