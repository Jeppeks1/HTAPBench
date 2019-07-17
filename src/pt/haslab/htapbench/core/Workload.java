package pt.haslab.htapbench.core;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.benchmark.*;
import pt.haslab.htapbench.benchmark.LatencyRecord.Sample;
import pt.haslab.htapbench.configuration.workload.Analytical;
import pt.haslab.htapbench.configuration.workload.Hybrid;
import pt.haslab.htapbench.types.State;
import pt.haslab.htapbench.util.Histogram;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Workload {

    private static final Logger LOG = Logger.getLogger(Workload.class);

    // Workload related
    private List<Thread> workerThreads = new ArrayList<Thread>();
    protected List<Worker> workers = new ArrayList<Worker>();
    protected List<Phase> phases = new ArrayList<Phase>();

    private WorkloadState workState;

    // Read-write lock for protecting shared state
    private final ReadWriteLock RWLock = new ReentrantReadWriteLock();

    // ClientBalancer
    private ClientBalancer balancer;
    private Thread threadBalancer;

    // Workload configuration
    protected HTAPBenchmark bench;
    private WorkloadConfiguration workConf;

    // Workload start and end time
    private long startTime;
    private long endTime;

    // WorkloadConfiguration variables
    private final int terminals;

    // -------------------------------------------------------------------
    //                    Workload configuration methods
    // -------------------------------------------------------------------

    public Workload(HTAPBenchmark bench) {
        this.workConf = bench.getWorkloadConfiguration();
        this.terminals = workConf.getTerminals();
        this.bench = bench;

        // Set the Phases of this Workload
        this.phases = bench.getWorkloadConfiguration().getAllPhases();

        // Prepare the ClientBalancer
        balancer = new ClientBalancer(bench, workers, this);
        threadBalancer = new Thread(balancer);
    }

    /**
     * Initiate a new benchmark and workload state.
     */
    private WorkloadState initializeState(BenchmarkState benchmarkState) {
        return new WorkloadState(benchmarkState, phases, terminals, workConf.getTraceReader());
    }

    // -------------------------------------------------------------------
    //                            Abstract method
    // -------------------------------------------------------------------

    /**
     * Initialize the workload-specific variables according to their class.
     */
    public abstract void initializeWorkers();

    // -------------------------------------------------------------------
    //                        Workload implementation
    // -------------------------------------------------------------------

    /**
     * Execute the current Workload against the database.
     */
    public List<Results> execute() {
        // Initialize the clock that should be used for the execution phase
        bench.initClock();

        // Initialize the Workload-dependent Workers
        initializeWorkers();

        // The benchmark- and workloadStates are published to the Workers and need synchronization
        final BenchmarkState benchmarkState = new BenchmarkState(workers.size() + 1);
        workState = initializeState(benchmarkState);

        // Create the initial Threads that are used for this benchmark
        createWorkerThreads(workState);
        benchmarkState.blockForStart();

        // Begin measuring the completion time
        startTime = System.nanoTime();

        // Switch to the first phase in the Phase list
        workState.switchToNextPhase();
        Phase phase = workState.getCurrentPhase();

        // Log information about the current phase
        LOG.info(phase.currentPhaseString());

        // Set the lowest observed phase-rate which is used to determine the sleep intervals
        int lowestRate = Integer.MAX_VALUE;
        if (phase.rate < lowestRate) {
            lowestRate = phase.rate;
        }

        // Determine the sleeping interval to meet the specified TPS
        long intervalNs = getInterval(lowestRate, phase.arrival);

        // Set the test- and warm-up duration in nanoseconds
        long testDurationNs = phase.time * 1000000000L;
        long warmupDurationNs = this instanceof Analytical ? 0 : 60 * 1000000000L; // One minute

        LOG.info("[Warm-up] Beginning warm-up phase for one minute");

        // Prepare values for the main loop
        long nextInterval = startTime + intervalNs;
        boolean resetQueues = true;
        int nextToAdd = 1;
        int rateFactor;

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

            // Check if the current phase is complete
            boolean phaseComplete;
            TraceReader tr = workConf.getTraceReader();
            if (tr != null) {
                // If a trace script is present, the phase is complete if the trace reader has no more
                assert workConf.getTraceReader() != null;
                if (!workState.getScriptPhaseComplete()) {
                    break;
                }

                phaseComplete = true;
            } else if (phase.isLatencyRun())
                // Latency runs (serial run through each query) have their own
                // state to mark completion
                phaseComplete = benchmarkState.getState() == State.LATENCY_COMPLETE;
            else {
                // Check if the current phase is complete
                phaseComplete = benchmarkState.getState() == State.MEASURE && (now - startTime >= testDurationNs);
            }

            // Go to next phase if this one is complete
            if (phaseComplete) {
                // enters here after each phase of the test
                // reset the queues so that the new phase is not affected by the
                // queue of the previous one
                resetQueues = true;

                // We are about to update the global benchmarkState - synchronize on it
                synchronized (benchmarkState) {
                    // Update the benchmark if the phase was a latency run
                    if (phase.isLatencyRun()) {
                        benchmarkState.ackLatencyComplete();
                    }

                    // Fetch a new Phase while synchronizing on the shared WorkloadState
                    synchronized (workState) {
                        workState.switchToNextPhase();
                        lowestRate = Integer.MAX_VALUE;
                        phase = workState.getCurrentPhase();
                        interruptWorkers();

                        // Check if there are any more phases left to benchmark
                        if (phase == null) {
                            // Last phase
                            endTime = now;
                            execute = false;
                            benchmarkState.startCoolDown();
                            LOG.info("[Workload] Waiting for all terminals to finish ..");

                            // Stop the ClientBalancer in case of a Hybrid workload
                            if (threadBalancer.isAlive()) {
                                balancer.terminate();
                                threadBalancer.interrupt();
                            }
                        } else {
                            phase.resetSerial();
                            LOG.info(phase.currentPhaseString());
                            if (phase.rate < lowestRate)
                                lowestRate = phase.rate;
                        }
                    }
                }

                if (phase != null) {
                    // update frequency in which we check according to wakeup speed
                    // intervalNs = (long) (1000000000. / (double) lowestRate + 0.5);
                    testDurationNs += phase.time * 1000000000L;
                }
            }

            // Compute the next interval and how many messages to deliver
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
            if (state == State.WARMUP && (now - startTime >= warmupDurationNs)) {
                if (phase != null && phase.isLatencyRun())
                    benchmarkState.startColdQuery();
                else
                    benchmarkState.startMeasure();

                startTime = benchmarkState.getTestStartNs();
                resetWorkerRequests();
                interruptWorkers();
                LOG.info("[Measure] Warmup complete, starting measurements.");

                // Start the ClientBalancer in case of a Hybrid workload
                if (this instanceof Hybrid)
                    threadBalancer.start();

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
            // Make sure all the workers ended
            finalizeWorkers(workerThreads);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        // The execution finished - begin collecting the results
        return collectResults();
    }

    // -------------------------------------------------------------------
    //                     Workload helper functions
    // -------------------------------------------------------------------

    private List<Results> collectResults() {
        List<Results> results = new ArrayList<Results>();

        // Distinguish between OLAP and OLTP workers to calculate statistics for each
        List<Worker> workersOLTP = new ArrayList<Worker>();
        List<Worker> workersOLAP = new ArrayList<Worker>();

        // Split the workers into two
        for (Worker w : workers) {
            if (w instanceof TPCCWorker)
                workersOLTP.add(w);
            else
                workersOLAP.add(w);
        }

        Results resultOLTP = prepareResults(workersOLTP, "TPCC");
        Results resultOLAP = prepareResults(workersOLAP, "TPCH");
        Results resultBalancer = balancer.getResults(isHybridWorkload());

        results.add(resultOLTP);
        results.add(resultOLAP);
        results.add(resultBalancer);

        return results;
    }

    private Results prepareResults(List<Worker> workersSplit, String name) {
        ArrayList<Sample> samples = new ArrayList<Sample>();

        // Return no results if the input list is empty
        if (workersSplit.isEmpty())
            return null;

        // Prepare a variable to count the number of requests
        int requests = 0;

        // Combine all the latencies together in the most disgusting way possible: sorting!
        for (Worker w : workersSplit) {
            // Examine the contents of the latency records
            if (w.getLatencyRecords() != null) {
                for (Sample sample : w.getLatencyRecords()) {
                    if (sample != null)
                        samples.add(sample);
                }
            }

            // Count the number of requests made to this worker
            requests += w.getRequests();
        }

        // Sort the samples
        Collections.sort(samples);

        // Compute stats on all the latencies
        long[] latencies = new long[samples.size()];
        for (int i = 0; i < samples.size(); ++i) {
            latencies[i] = samples.get(i).getLatencyUs();
        }

        DistributionStatistics stats = DistributionStatistics.computeStatistics(latencies);

        Results results = new Results(endTime - startTime, requests, stats, samples);
        results.setName(name);

        // Compute transaction histogram
        Set<TransactionType> txnTypes = new HashSet<TransactionType>();
        txnTypes.addAll(workConf.getTransTypes());
        txnTypes.remove(TransactionType.INVALID);

        results.txnSuccess.putAll(txnTypes, 0);
        results.txnRetry.putAll(txnTypes, 0);
        results.txnAborted.putAll(txnTypes, 0);
        results.txnErrors.putAll(txnTypes, 0);

        for (Worker w : workersSplit) {
            results.txnSuccess.putHistogram(w.getSuccessHistogram());
            results.txnAborted.putHistogram(w.getAbortedHistogram());
            results.txnErrors.putHistogram(w.getErrorHistogram());
            results.txnRetry.putHistogram(w.getRetryHistogram());

            if (w.getWorkloadConfiguration().getCalibrate()) {
                if (w instanceof TPCCWorker)
                    results.setTsCounter(((TPCCWorker) w).getTsCounter().get());
            }

            for (Map.Entry<TransactionType, Histogram<String>> e : w.getRecordedMessagesHistogram().entrySet()) {
                Histogram<String> h = results.txnRecordedMessages.get(e.getKey());
                if (h == null) {
                    h = new Histogram<String>(true);
                    results.txnRecordedMessages.put(e.getKey(), h);
                }

                h.putHistogram(e.getValue());
            }
        }

        return results;
    }

    private void finalizeWorkers(List<Thread> workerThreads) throws InterruptedException {
        // Interrupt any workers that have still not finished
        interruptWorkers();

        // Wait for the workers to finish
        for (Thread workerThread : workerThreads) {
            LOG.debug("Worker: [" + workerThread.getName() + "] is finalizing");
            workerThread.join(); // Wait for the current query to hopefully finish
            LOG.debug("Worker: [" + workerThread.getName() + "] has ended");
        }

        LOG.info("[Workload] All terminals have finished ..");
    }

    private void interruptWorkers() {
        // Aqcuire the readlock before attempting to cancel the statements,
        // to make sure the ClientBalancer does not add a new worker in the
        // middle of the iteration.
        RWLock.readLock().lock();
        for (Worker worker : workers) {
            worker.cancelStatement();
        }

        // Release the lock
        RWLock.readLock().unlock();
    }

    private void resetWorkerRequests() {
        // Reset the accumulated transaction count for each worker, when switching
        // from the warm-up phase to the measure phase. The RW lock is not necessary,
        // as the ClientBalancer will not have been invoked at this point.
        for (Worker worker : workers)
            worker.getTxncount();

        // Clear the workQueue in case the warm-up phase experienced back-pressure,
        // to not promote a sudden burst in throughput when the start-up latency
        // settles down.
        workState.addToQueue(0, true, workers.get(0));
    }

    private void createWorkerThreads(WorkloadState workloadState) {
        for (Worker worker : workers) {
            worker.initializeState(workloadState);
            Thread thread = new Thread(worker);
            thread.start();
            workerThreads.add(thread);
        }
    }

    void addTPCHWorker(int currentCount) {
        // Create a worker and initialize it with the shared WorkloadState
        Worker worker = bench.makeOLAPWorker();
        worker.initializeState(workState);

        // The ClientBalancer is likely to conflict on the workers list with
        // the iteration in interruptWorkers(). Guard for this possibility by
        // using a ReadWriteLock.
        RWLock.writeLock().lock();
        workers.add(worker);
        RWLock.writeLock().unlock();

        // Create a thread and start it
        Thread thread = new Thread(worker);

        // Simulate error in original implementation where only one TPCHWorker
        // actually gets invoked through the start method on the Thread.
        if (currentCount == 0) {
            thread.start();
            workerThreads.add(thread);
        }
    }

    private long getInterval(int lowestRate, Phase.Arrival arrival) {
        if (arrival == Phase.Arrival.POISSON)
            return (long) ((-Math.log(1 - Math.random()) / lowestRate) * 1000000000.);
        else
            return (long) (1000000000. / (double) lowestRate + 0.5);
    }

    // -------------------------------------------------------------------
    //                     Workload specific information
    // -------------------------------------------------------------------

    private boolean isRateLimited() {
        return this instanceof Hybrid;
    }

    private boolean isHybridWorkload() {
        return this instanceof Hybrid;
    }
}
