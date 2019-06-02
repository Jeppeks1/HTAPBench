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

import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.densitity.Clock;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements the ClientBalacer which is responsible for deciding if more OLAP streams should be added to system.
 * The decision is bounded to a configurable error margin. We accept to degrade up to ERROR_MARGIN(%) of the OLTP
 * targetTPS in trade to add another OLAP Stream.
 */
public class ClientBalancer implements Runnable {
    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(ClientBalancer.class);

    private final BenchmarkModule benchmarkModule;
    protected final Connection conn;
    private Results results;

    private List<Worker> workersOLTP;
    private List<Worker> workersOLAP;

    //sampling rate;
    private final int deltaT = 60;
    private double error_margin;

    private int TPM = 0;
    private int previous_TPS = 0;
    private int projected_TPM;
    private int olapStreams = 0;
    private double integral = 0;
    private boolean saturated = false;
    private boolean terminate = false;

    private Clock clock;

    private LatencyRecord latencies;
    private ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();

    // Interval requests used by the monitor
    private AtomicInteger intervalRequests = new AtomicInteger(0);

    ClientBalancer(BenchmarkModule benchModule, List<Worker> workersOLTP, List<Worker> workersOLAP, WorkloadConfiguration wrkld, Clock clock, double error_margin) {
        int targetTPS = benchModule.getWorkloadConfiguration().getTargetTPS();
        this.projected_TPM = targetTPS * this.deltaT;
        this.benchmarkModule = benchModule;

        this.workersOLTP = workersOLTP;
        this.workersOLAP = workersOLAP;
        this.error_margin = error_margin;
        this.clock = clock;

        this.latencies = new LatencyRecord(System.nanoTime());

        try {
            this.conn = this.benchmarkModule.makeConnection();
            this.conn.setAutoCommit(false);
            this.conn.setTransactionIsolation(wrkld.getIsolationMode());
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }
    }

    /**
     * Computes the current TPM count by reading all workers stats.
     */
    private void getTPM() {
        int txn_count = 0;
        for (Worker w : workersOLTP) {
            txn_count = txn_count + w.getTxncount();
        }
        this.TPM = txn_count;
    }

    @Override
    public void run() {
        try {
            long measureStart = System.nanoTime();
            int requests = 0;
            this.latencies.addLatency(workersOLAP.size(), 0, 0, this.TPM, 0);
            intervalRequests.incrementAndGet();

            while (!terminate) {
                requests++;
                Thread.sleep(deltaT * 1000);
                //Reads and computes the current observed TPM.
                getTPM();

                double error = this.projected_TPM - this.TPM;
                this.integral = this.integral + error / this.deltaT;

                double ki = 0.03;
                double kp = 0.4;
                double output = kp * error + ki * this.integral;

                /*
                 * Take decision. If the the total targetTPS is within the
                 * error margin --> Launch another OLAP Stream.
                 */
                LOG.info("TPM: " + this.TPM);
                LOG.info("output: " + output);
                LOG.info("error: " + error);

                if (this.olapStreams == 0 || (!saturated && output < this.error_margin * this.projected_TPM)) {
                    this.olapStreams++;

                    this.workersOLAP.addAll(benchmarkModule.makeOLAPWorker(clock));
                    LOG.info("ClientBalancer: Going to lauch 1 OLAP stream. Total OLAP STreams: " + workersOLAP.size());
                } else {
                    saturated = true;
                    LOG.info("***************************************************************************************************");
                    LOG.info("         ClientBalancer: The system is saturated. No more OLAP streams will be lauched.");
                    LOG.info("***************************************************************************************************");
                }

                LOG.info("***************************************************************************************************");
                LOG.info("                          #ACTIVE OLAP STREAMS: " + workersOLAP.size());
                LOG.info("***************************************************************************************************");

                this.latencies.addLatency(workersOLAP.size(), 0, 0, this.TPM, 0);
                intervalRequests.incrementAndGet();
            }

            if (terminate) {
                long measureEnd = System.nanoTime();

                for (LatencyRecord.Sample sample : getLatencyRecords()) {
                    if (sample != null)
                        samples.add(sample);
                }
                Collections.sort(samples);

                // Compute stats on all the latencies
                int[] latencies = new int[samples.size()];
                for (int i = 0; i < samples.size(); ++i) {
                    latencies[i] = samples.get(i).latencyUs;
                }
                DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

                results = new Results(measureEnd - measureStart, requests, stats, samples);
                results.setName("CLIENT BALANCER");
            }
        } catch (InterruptedException | IOException ex) {
            Logger.getLogger(ClientBalancer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Results getResults() {
        return results;
    }

    void terminate() {
        this.terminate = true;
    }

    private Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return this.latencies;
    }
}
