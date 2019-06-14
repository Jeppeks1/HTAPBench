
/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************
 /*
 * Copyright 2017 by INESC TEC                                                                                                
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.benchmark.TPCCWorker;
import pt.haslab.htapbench.util.Histogram;
import pt.haslab.htapbench.util.StringUtil;
import pt.haslab.htapbench.types.State;

public class ThreadBench implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = Logger.getLogger(ThreadBench.class);

    private ArrayList<LatencyRecord.Sample> samples = new ArrayList<LatencyRecord.Sample>();
    private final ArrayList<Thread> workerThreads;
    private final List<? extends Worker> workers;
    private WorkloadConfiguration workConf;
    private int intervalMonitor;
    private boolean isTPCC;

    private ThreadBench(List<? extends Worker> workers, WorkloadConfiguration workConf, int intervalMonitor) {
        this.workerThreads = new ArrayList<Thread>(workers.size());
        this.intervalMonitor = intervalMonitor;
        this.workConf = workConf;
        this.workers = workers;

        // Determine if the current invocation of ThreadBench is for TPCC or TPCH.
        // TPCC workers are created in HTAPBench, meaning the workers parameter is non-empty.
        // If the workers list is non-empty, we have to check the class instance.
        isTPCC = !workers.isEmpty() && workers.get(0) instanceof TPCCWorker;
    }

    private void createWorkerThreads() {
        for (Worker worker : workers) {
            worker.initializeState();
            Thread thread = new Thread(worker);
            thread.setUncaughtExceptionHandler(this);
            thread.start();
            this.workerThreads.add(thread);
        }
    }

    private void interruptWorkers() {
        for (Worker worker : workers) {
            LOG.info("Worker: [" + worker.getName() + "] is canceling statement");
            worker.cancelStatement();
        }

    }

    private int finalizeWorkers(ArrayList<Thread> workerThreads, BenchmarkState testState) throws InterruptedException {
        assert testState.getState() == State.DONE || testState.getState() == State.EXIT;
        int requests = 0;

        new WatchDogThread(testState).start();

        for (int i = 0; i < workerThreads.size(); ++i) {
            LOG.debug("Worker: [" + workerThreads.get(i).getName() + "] is finalizing");
            workerThreads.get(i).join(30000);
            requests += workers.get(i).getRequests();
            workers.get(i).tearDown();
            LOG.debug("Worker: [" + workerThreads.get(i).getName() + "] is no longer joining");
        }

        return requests;
    }

    private class WatchDogThread extends Thread {
        private BenchmarkState testState;

        WatchDogThread(BenchmarkState testState) {
            this.setDaemon(true);
            this.testState = testState;
        }

        @Override
        public void run() {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            LOG.info("Starting WatchDogThread");
            while (true) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    return;
                }
                if (testState == null)
                    return;
                m.clear();
                for (Thread t : workerThreads) {
                    m.put(t.getName(), t.isAlive());
                }
                LOG.debug("Worker Thread Status:\n" + StringUtil.formatMaps(m));
            } // WHILE
        }
    } // CLASS

    private class MonitorThread extends Thread {
        private final int intervalMonitor;
        private final BenchmarkState testState;

        MonitorThread(int interval, BenchmarkState testState) {
            this.setDaemon(true);
            this.intervalMonitor = interval;
            this.testState = testState;
        }

        @Override
        public void run() {
            LOG.info("Starting MonitorThread Interval[" + this.intervalMonitor + " seconds]");
            while (true) {
                try {
                    Thread.sleep(this.intervalMonitor * 1000);
                } catch (InterruptedException ex) {
                    return;
                }
                if (testState == null)
                    return;
                // Compute the last throughput
                long measuredRequests = 0;
                synchronized (testState) {
                    for (Worker w : workers) {
                        measuredRequests += w.getAndResetIntervalRequests();
                    }
                }
                double tps = (double) measuredRequests / (double) this.intervalMonitor;
                LOG.info("Throughput: " + tps + " Tps");
            } // WHILE
        }
    } // CLASS

    static Results runThreadBench(List<Worker> workers, WorkloadConfiguration workConfs, int intervalMonitoring) {
        ThreadBench bench = new ThreadBench(workers, workConfs, intervalMonitoring);
        bench.intervalMonitor = intervalMonitoring;
        return bench.runRateLimitedMultiPhase();
    }

    private Results runRateLimitedMultiPhase() {
        final BenchmarkState benchmarkState = new BenchmarkState(workers.size() + 1);
        final WorkloadState workState = workConf.initializeState(benchmarkState);

        String letter = (isTPCC ? "C" : "H");

        createWorkerThreads();
        benchmarkState.blockForStart();

        int lowestRate = Integer.MAX_VALUE; // used to determine the longest sleep interval
        long start = System.nanoTime();
        long startOlap = start; // OLAP does not have a warm-up phase, so duplicate start time.
        long endTime = -1;

        workState.switchToNextPhase();
        Phase phase = workState.getCurrentPhase();
        phase.offsetTime(isTPCC);

        LOG.info(phase.currentPhaseString());

        if (phase.rate < lowestRate) {
            lowestRate = phase.rate;
        }

        long intervalNs = getInterval(lowestRate, phase.arrival);

        long nextInterval = start + intervalNs;
        int nextToAdd = 1;
        int rateFactor;

        boolean resetQueues = true;

        long testDurationNs = phase.time * 1000000000L;

        // Initialize the Monitor
        if (this.intervalMonitor > 0) {
            new MonitorThread(this.intervalMonitor, benchmarkState).start();
        }

        // Main Loop
        boolean execute = true;
        while (execute) {
            // posting new work... and reseting the queue in case we have new
            // portion of the workload...

            if (workState.getCurrentPhase() != null) {
                rateFactor = workState.getCurrentPhase().rate / lowestRate;
            } else {
                rateFactor = 1;
            }
            workState.addToQueue(nextToAdd * rateFactor, resetQueues, workers.get(0));
            resetQueues = false;

            // Wait until the interval expires, which may be "don't wait"
            long now = System.nanoTime();
            long diff = nextInterval - now;
            while (diff > 0) { // this can wake early: sleep multiple times to
                // avoid that
                long ms = diff / 1000000;
                diff = diff % 1000000;
                try {
                    Thread.sleep(ms, (int) diff);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                now = System.nanoTime();
                diff = nextInterval - now;
            }

            boolean phaseComplete = false;
            if (phase != null) {
                TraceReader tr = workConf.getTraceReader();
                if (tr != null) {
                    // If a trace script is present, the phase is complete if the trace reader has no more
                    assert workConf.getTraceReader() != null;
                    if (!workConf.getWorkloadState().getScriptPhaseComplete()) {
                        break;
                    }
                    phaseComplete = true;
                } else if (phase.isLatencyRun())
                    // Latency runs (serial run through each query) have their own
                    // state to mark completion
                    phaseComplete = benchmarkState.getState() == State.LATENCY_COMPLETE;
                else {
                    // Check if the current phase is complete
                    if (isTPCC)
                        phaseComplete = benchmarkState.getState() == State.MEASURE && (now - start >= testDurationNs);
                    else
                        phaseComplete = (now - start >= testDurationNs);
                }
            }

            // Go to next phase if this one is complete
            if (phaseComplete) {
                // enters here after each phase of the test
                // reset the queues so that the new phase is not affected by the
                // queue of the previous one
                resetQueues = true;

                // Fetch a new Phase
                synchronized (benchmarkState) {
                    if (phase.isLatencyRun()) {
                        benchmarkState.ackLatencyComplete();
                    }
                    synchronized (workState) {
                        workState.switchToNextPhase();
                        lowestRate = Integer.MAX_VALUE;
                        phase = workState.getCurrentPhase();
                        interruptWorkers();

                        if (phase == null) {
                            // Last phase
                            endTime = now;
                            execute = false;
                            benchmarkState.startCoolDown();
                            LOG.info("[TPC_" + letter + " Terminate] Waiting for all terminals to finish ..");
                        } else {
                            phase.resetSerial();
                            LOG.info(phase.currentPhaseString());
                            if (phase.rate < lowestRate) {
                                lowestRate = phase.rate;
                            }
                        }
                    }
                    if (phase != null) {
                        // update frequency in which we check according to
                        // wakeup
                        // speed
                        // intervalNs = (long) (1000000000. / (double)
                        // lowestRate + 0.5);
                        testDurationNs += phase.time * 1000000000L;
                    }
                }
            }

            // Compute the next interval
            // and how many messages to deliver
            if (phase != null) {
                intervalNs = 0;
                nextToAdd = 0;
                do {
                    intervalNs += getInterval(lowestRate, phase.arrival);
                    nextToAdd++;
                } while ((-diff) > intervalNs);
                nextInterval += intervalNs;
            }

            // Update the test state appropriately
            State state = benchmarkState.getState();
            if (state == State.WARMUP && now >= start) {
                synchronized (benchmarkState) {
                    if (phase != null && phase.isLatencyRun())
                        benchmarkState.startColdQuery();
                    else
                        benchmarkState.startMeasure();
                    interruptWorkers();
                }
                start = now;
                LOG.info("[Measure] Warmup complete, starting measurements.");
                // measureEnd = measureStart + measureSeconds * 1000000000L;

                // For serial executions, we want to do every query exactly
                // once, so we need to restart in case some of the queries
                // began during the warmup phase.
                // If we're not doing serial executions, this function has no
                // effect and is thus safe to call regardless.
                phase.resetSerial();
            } else if (state == State.EXIT) {
                // All threads have noticed the done, meaning all measured
                // requests have definitely finished.
                // Time to quit.
                break;
            }
        }

        try {
            int requests = finalizeWorkers(this.workerThreads, benchmarkState);

            // Combine all the latencies together in the most disgusting way possible: sorting!
            for (Worker w : workers) {
                if (w.getLatencyRecords() != null) {
                    for (LatencyRecord.Sample sample : w.getLatencyRecords()) {
                        if (sample != null)
                            samples.add(sample);
                    }
                }
            }

            Collections.sort(samples);

            // Compute stats on all the latencies
            int[] latencies = new int[samples.size()];
            for (int i = 0; i < samples.size(); ++i) {
                latencies[i] = samples.get(i).latencyUs;
            }
            DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

            start = isTPCC ? start : startOlap; // Adjust time measurement according to TPCC or TPCH
            Results results = new Results(endTime - start, requests, stats, samples);
            results.setName("TPC" + letter);

            // Compute transaction histogram
            Set<TransactionType> txnTypes = new HashSet<TransactionType>();
            txnTypes.addAll(workConf.getTransTypes());
            txnTypes.remove(TransactionType.INVALID);

            results.txnSuccess.putAll(txnTypes, 0);
            results.txnRetry.putAll(txnTypes, 0);
            results.txnAborted.putAll(txnTypes, 0);
            results.txnErrors.putAll(txnTypes, 0);

            for (Worker w : workers) {
                results.txnSuccess.putHistogram(w.getTransactionSuccessHistogram());
                results.txnRetry.putHistogram(w.getTransactionRetryHistogram());
                results.txnAborted.putHistogram(w.getTransactionAbortHistogram());
                results.txnErrors.putHistogram(w.getTransactionErrorHistogram());

                if (w.getWorkloadConfiguration().getCalibrate()) {
                    if (w instanceof TPCCWorker)
                        results.setTsCounter(((TPCCWorker) w).getTs_conter().get());
                }

                for (Entry<TransactionType, Histogram<String>> e : w.getTransactionAbortMessageHistogram().entrySet()) {
                    Histogram<String> h = results.txnRecordedMessages.get(e.getKey());
                    if (h == null) {
                        h = new Histogram<String>(true);
                        results.txnRecordedMessages.put(e.getKey(), h);
                    }
                    h.putHistogram(e.getValue());
                }
            }

            return results;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private long getInterval(int lowestRate, Phase.Arrival arrival) {
        if (arrival == Phase.Arrival.POISSON)
            return (long) ((-Math.log(1 - Math.random()) / lowestRate) * 1000000000.);
        else
            return (long) (1000000000. / (double) lowestRate + 0.5);
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
    }


}
