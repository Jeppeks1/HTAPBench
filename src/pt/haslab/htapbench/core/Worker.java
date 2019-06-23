
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

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.Procedure;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.benchmark.BenchmarkModule;
import pt.haslab.htapbench.catalog.Catalog;
import pt.haslab.htapbench.benchmark.LatencyRecord;
import pt.haslab.htapbench.benchmark.SubmittedProcedure;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.procedures.tpch.GenericQuery.InvalidResultException;
import pt.haslab.htapbench.configuration.Configuration;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.types.ResultSetResult;
import pt.haslab.htapbench.types.State;
import pt.haslab.htapbench.types.TransactionStatus;
import pt.haslab.htapbench.util.Histogram;

import static pt.haslab.htapbench.types.TransactionStatus.SUCCESS;

public abstract class Worker implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);

    private WorkloadState wrkldState;
    private LatencyRecord latencies;
    private Statement currStatement;
    private final int id;

    // Interval requests used by the monitor
    private AtomicInteger intervalRequests = new AtomicInteger(0);
    private AtomicInteger txncount = new AtomicInteger(0);

    private final TransactionTypes transactionTypes;
    private final BenchmarkModule benchmarkModule;
    final WorkloadConfiguration wrkld;
    protected final Configuration config;
    protected final Connection conn;

    private final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<Class<? extends Procedure>, Procedure>();
    protected final Map<TransactionType, Procedure> procedures = new HashMap<TransactionType, Procedure>();
    private final Map<String, Procedure> name_procedures = new HashMap<String, Procedure>();

    private final Map<TransactionType, Histogram<String>> txnRecordedMessages = new HashMap<TransactionType, Histogram<String>>();
    private final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>();
    private final Histogram<TransactionType> txnAborted = new Histogram<TransactionType>();
    private final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>();
    private final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>();

    private boolean seenDone = false;

    public Worker(BenchmarkModule benchmarkModule, int id) {
        this.wrkld = benchmarkModule.getWorkloadConfiguration();
        this.config = benchmarkModule.getConfiguration();
        this.transactionTypes = wrkld.getTransTypes();
        this.wrkldState = wrkld.getWorkloadState();
        this.benchmarkModule = benchmarkModule;
        this.currStatement = null;
        this.id = id;

        // Check if the TransactionTypes are not null
        assert (transactionTypes != null) : "The TransactionTypes from the WorkloadConfiguration is null!";

        // Initialize the connection handle for execution
        conn = config.prepareExecution();

        // Generate all the Procedures that we're going to need
        procedures.putAll(benchmarkModule.getProcedures());

        // Make sure we managed to retrieve all Procedures
        assert (procedures.size() == transactionTypes.size()) :
                String.format("Failed to get all of the Procedures for %s [expected=%d, actual=%d]",
                        benchmarkModule.getBenchmarkName(),
                        transactionTypes.size(),
                        procedures.size());

        // Populate the maps
        for (Entry<TransactionType, Procedure> e : procedures.entrySet()) {
            Procedure proc = e.getValue();
            name_procedures.put(e.getKey().getName(), proc);
            class_procedures.put(proc.getClass(), proc);
        }
    }

    /**
     * Get the unique thread id for this worker
     */
    public final int getId() {
        return this.id;
    }

    final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.benchmarkModule.getWorkloadConfiguration());
    }

    public final Catalog getCatalog() {
        return (this.benchmarkModule.getCatalog());
    }

    public final Connection getConnection() {
        return (this.conn);
    }

    final int getRequests() {
        return latencies.size();
    }

    final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }

    final Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return latencies;
    }

    /**
     * Returns the current txn count on this worker. Before returning the value,
     * the counter is set to 0. This avoids storing past counter values in the client balancer.
     */
    int getTxncount() {
        int txn = txncount.get();
        txncount.set(0);
        return txn;
    }

    @SuppressWarnings("unchecked")
    final <T extends Procedure> T getProcedure(Class<T> procClass) {
        return (T) (class_procedures.get(procClass));
    }

    public final Histogram<TransactionType> getSuccessHistogram() {
        return (this.txnSuccess);
    }

    public final Histogram<TransactionType> getRetryHistogram() {
        return (this.txnRetry);
    }

    public final Histogram<TransactionType> getAbortedHistogram() {
        return (this.txnAborted);
    }

    public final Histogram<TransactionType> getErrorHistogram() {
        return (this.txnErrors);
    }

    public final Map<TransactionType, Histogram<String>> getRecordedMessagesHistogram() {
        return (this.txnRecordedMessages);
    }

    synchronized public void setCurrStatement(Statement s) {
        this.currStatement = s;
    }

    /**
     * Stop executing the current statement.
     */
    synchronized void cancelStatement() {
        try {
            if (currStatement != null){
                LOG.debug("Worker: [" + this.getName() + "] is canceling statement");
                currStatement.cancel();
            }
        } catch (SQLException e) {
            LOG.error("Failed to cancel statement: " + e.getMessage());
        }
    }

    /**
     * Get unique name for this worker's thread
     */
    public final String getName() {
        String classname = this.getClass().getSimpleName();

        switch (classname) {
            case "TPCCWorker":
                return String.format("TPCCWorker%03d", this.getId());
            case "TPCHWorker":
                return String.format("TPCHWorker%03d", this.getId());
            default:
                return Thread.currentThread().getName();
        }
    }

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        SubmittedProcedure pieceOfWork;
        t.setName(this.getName());

        // In case of reuse reset the measurements
        latencies = new LatencyRecord(wrkldState.getTestStartNs());

        // Invoke the initialize callback
        try {
            this.initialize();
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when initializing " + this.getName(), ex);
        }

        // wait for start
        wrkldState.blockForStart();
        State preState, postState;
        Phase phase;

        TransactionType invalidTT = TransactionType.INVALID;

        work:
        while (true) {

            // PART 1: Init and check if done

            preState = wrkldState.getGlobalState();

            switch (preState) {
                case DONE:
                    if (!seenDone) {
                        // This is the first time we have observed that the
                        // test is done notify the global test state, then
                        // continue applying load
                        seenDone = true;
                        wrkldState.signalDone();
                        break work;
                    }
                    break;
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            wrkldState.stayAwake();
            phase = wrkldState.getCurrentPhase();
            if (phase == null)
                continue work;

            // Fetch the next piece of work - wait if necessary to not exceed the target tps
            pieceOfWork = wrkldState.fetchWork(this);
            preState = wrkldState.getGlobalState();

            phase = wrkldState.getCurrentPhase();
            if (phase == null)
                continue work;

            switch (preState) {
                case DONE:
                case EXIT:
                case LATENCY_COMPLETE:
                    // Once a latency run is complete, we wait until the next
                    // phase or until DONE.
                    continue work;
            }

            // PART 3: Execute work
            long start = pieceOfWork.getStartTime();

            TransactionType type = invalidTT;
            ResultSetResult rows = new ResultSetResult();
            try {
                type = doWork(pieceOfWork, rows);
            } catch (IndexOutOfBoundsException e) {
                if (phase.isThroughputRun()) {
                    LOG.error("Thread tried executing disabled phase!");
                    throw e;
                }

                if (phase.id == wrkldState.getCurrentPhase().id) {
                    switch (preState) {
                        case WARMUP:
                            // Don't quit yet: we haven't even begun!
                            phase.resetSerial();
                            break;
                        case COLD_QUERY:
                        case MEASURE:
                            // The serial phase is over. Finish the run early.
                            wrkldState.signalLatencyComplete();
                            LOG.info("[Serial] Serial execution of all transactions complete.");
                            break;
                        default:
                            throw e;
                    }
                }
            }

            // PART 4: Record results

            long end = System.nanoTime();
            postState = wrkldState.getGlobalState();

            switch (postState) {
                case MEASURE:
                    // Non-serial measurement. Only measure if the state both
                    // before and after was MEASURE, and the phase hasn't
                    // changed, otherwise we're recording results for a query
                    // that either started during the warmup phase or ended
                    // after the timer went off.
                    if (preState == State.MEASURE && type != null && wrkldState.getCurrentPhase().id == phase.id) {
                        // Record the result
                        latencies.addLatency(type.getId(), start, end, id, rows.getRows());

                        // Increment the number of requests in this interval for TPS control purposes
                        intervalRequests.incrementAndGet();

                        // Write the query information to the log for TPCHWorkers
                        if (this instanceof TPCHWorker)
                            LOG.debug("Stat [Txn ID,latency,rows]: " + type.getName() + ", " + (end - start) + ", " + rows.getRows());
                    }

                    // Start a cold query in case of latency run
                    if (phase.isLatencyRun())
                        wrkldState.startColdQuery();

                    break;
                case COLD_QUERY:
                    // No recording for cold runs, but next time we will, since it'll be a hot run.
                    if (preState == State.COLD_QUERY)
                        wrkldState.startHotQuery();
                    break;
            }

            wrkldState.finishedWork();
        }

        tearDown();
    }

    /**
     * Called in a loop in the thread to exercise the system under test.
     * Each implementing worker should return the TransactionType handle that
     * was executed.
     */
    private TransactionType doWork(SubmittedProcedure pieceOfWork, ResultSetResult rows) {
        TransactionType next = null;
        TransactionStatus status = TransactionStatus.RETRY;
        final DatabaseType dbType = wrkld.getDBType();

        try {
            while (status == TransactionStatus.RETRY && this.wrkldState.getGlobalState() != State.DONE) {
                if (next == null) {
                    next = transactionTypes.getType(pieceOfWork.getType());
                }

                assert (!next.isSupplemental()) : "Trying to select a supplemental transaction " + next;

                try {
                    status = executeWork(next, rows);
                } catch (SQLException ex) {
                    // Handle acceptable database specific SQLExceptions
                    status = handleSQLException(next, ex);
                } finally {
                    // Increment the transaction counter, as it represents started transactions
                    txncount.incrementAndGet();

                    // Perform a rollback if the status is not SUCCESS and otherwise commit.
                    if (!(status == SUCCESS))
                        conn.rollback();
                    else
                        conn.commit();

                    // Determine which logging actions should be taken based on the status.
                    switch (status) {
                        case SUCCESS:
                            txnSuccess.put(next);
                            break;
                        case ABORTED:
                            txnAborted.put(next);
                            break;
                        case RETRY:
                            txnRetry.put(next);
                            break;
                        case INTERRUPTED:
                            LOG.debug(getName() + " interrupted transaction " + next + " due to benchmark or user request");
                            break;
                        case INVALID_RESULT:
                            // This should be treated as an outright error due to query validation
                            txnErrors.put(next);
                            break;
                        case UNKNOWN_EXCEPTION:
                            LOG.warn("Caught an unknown SQLException in " + next.getName() + ". Examine the recorded error messages.");
                            txnErrors.put(next);
                            break;
                        default:
                            assert (false) : String.format("Unexpected status '%s' for %s", status, next);
                    } // SWITCH
                }
            } // WHILE
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error in %s when executing %s [%s]",
                    this.getName(), next, dbType), ex);
        }

        return next;
    }

    /**
     * Optional callback that can be used to initialize the Worker
     * right before the benchmark execution begins
     */
    protected void initialize() {
        // The default is to do nothing
    }

    /**
     * Invoke a single transaction for the given TransactionType
     */
    protected abstract TransactionStatus executeWork(TransactionType txnType, ResultSetResult rows) throws SQLException;

    /**
     * Called at the end of the test to do any clean up that may be required.
     */
    public void tearDown() {
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.warn("No connection to close");
        }
    }

    void initializeState() {
        assert (this.wrkldState == null);
        this.wrkldState = this.wrkld.getWorkloadState();
    }

    /**
     * Records the given exception message in a Histogram and associates
     * the message with the TransactionType that caused the exception.
     * If the offending SQL statement is given in the form of an
     * InvalidResultException, the statement will be debug-logged.
     *
     * @param next The TransactionType that was running when an Exception
     *             was thrown.
     * @param ex The Exception that was thrown.
     */
    void recordMessage(TransactionType next, Exception ex) {
        // Handle the logging
        if (ex instanceof InvalidResultException)
            LOG.debug("Query " + next + " resulted in exception:\n" + ex.getMessage()
                    + " for statement:\n" + ((InvalidResultException) ex).getStmt());
        else
            LOG.debug("Caught exception: " + ex.getMessage());

        // Record the message to be able to report on all exceptions at once
        if (wrkld.getRecordAbortMessages()) {
            Histogram<String> error_h = txnRecordedMessages.get(next);
            if (error_h == null) {
                error_h = new Histogram<String>();
                txnRecordedMessages.put(next, error_h);
            }

            error_h.put(ex.getMessage());
        }
    }

    /**
     * Database-system specific SQLException handling
     *
     * @return the TransactionStatus after handling the SQLException.
     */
    private TransactionStatus handleSQLException(TransactionType next, SQLException ex) {
        TransactionStatus status;

        // This is the only SQLException that should not be recorded as an error
        if (ex.getMessage().contains("cancelled due to client request")) {
            // The benchmark or the user requested the transaction to abort. This
            // occurs when the benchmark state is updated and interruptWorkers is
            // called or if the user manually enters a "kill" command.
            return TransactionStatus.INTERRUPTED;
        }

        // Record the error message
        recordMessage(next, ex);
        txnErrors.put(next);

        // Update the status
        if (ex.getSQLState() == null) {
            status = TransactionStatus.ABORTED;
        } else if (ex.getErrorCode() == 1213 && ex.getSQLState().equals("40001")) {
            // MySQLTransactionRollbackException
            status = TransactionStatus.ABORTED;
        } else if (ex.getErrorCode() == 1205 && ex.getSQLState().equals("41000")) {
            // MySQL Lock timeout
            status = TransactionStatus.ABORTED;
        } else if (ex.getErrorCode() == 1205 && ex.getSQLState().equals("40001")) {
            // SQLServerException Deadlock
            status = TransactionStatus.ABORTED;
        } else if (ex.getErrorCode() == -911 && ex.getSQLState().equals("40001")) {
            // DB2Exception Deadlock
            status = TransactionStatus.ABORTED;
        } else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("40001")) {
            // Postgres serialization
            status = TransactionStatus.ABORTED;
        } else if (ex.getErrorCode() == 8177 && ex.getSQLState().equals("72000")) {
            // ORA-08177: Oracle Serialization
            status = TransactionStatus.ABORTED;
        } else if ((ex.getErrorCode() == 0 && ex.getSQLState().equals("57014"))
                || (ex.getErrorCode() == -952 && ex.getSQLState().equals("57014"))) {
            // DB2: Query cancelled by benchmark because we changed state.
            // That's fine! We expected/caused this.
            status = TransactionStatus.ABORTED;
        } else {
            // Unknown error
            status = TransactionStatus.UNKNOWN_EXCEPTION;
        }

        return status;
    }
}
