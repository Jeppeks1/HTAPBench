
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
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.Procedure;
import pt.haslab.htapbench.api.Procedure.UserAbortException;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.benchmark.BenchmarkModule;
import pt.haslab.htapbench.catalog.Catalog;
import pt.haslab.htapbench.benchmark.LatencyRecord;
import pt.haslab.htapbench.benchmark.SubmittedProcedure;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.types.ResultSetResult;
import pt.haslab.htapbench.types.State;
import pt.haslab.htapbench.types.TransactionStatus;
import pt.haslab.htapbench.util.Histogram;
import pt.haslab.htapbench.util.StringUtil;

public abstract class Worker implements Runnable {
    private static final Logger LOG = Logger.getLogger(Worker.class);

	private WorkloadState wrkldState;
	private LatencyRecord latencies;
        private Statement currStatement;
    
        // Interval requests used by the monitor
        private AtomicInteger intervalRequests = new AtomicInteger(0);
        private AtomicInteger txncount = new AtomicInteger(0);
	
	private final int id;
	private final BenchmarkModule benchmarkModule;
	protected final Connection conn;
	protected final WorkloadConfiguration wrkld;
	protected final TransactionTypes transactionTypes;
	protected final Map<TransactionType, Procedure> procedures = new HashMap<TransactionType, Procedure>();
	protected final Map<String, Procedure> name_procedures = new HashMap<String, Procedure>();
	protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<Class<? extends Procedure>, Procedure>();
	
	private final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>();
	private final Histogram<TransactionType> txnAbort = new Histogram<TransactionType>();
	private final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>();
	private final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>();
	private final Map<TransactionType, Histogram<String>> txnAbortMessages = new HashMap<TransactionType, Histogram<String>>();
	
	private boolean seenDone = false;
	
	public Worker(BenchmarkModule benchmarkModule, int id) {
		this.id = id;
		this.benchmarkModule = benchmarkModule;
		this.wrkld = this.benchmarkModule.getWorkloadConfiguration();
		this.wrkldState = this.wrkld.getWorkloadState();
                this.currStatement = null;
		this.transactionTypes = this.wrkld.getTransTypes();
		assert(this.transactionTypes != null) :
		    "The TransactionTypes from the WorkloadConfiguration is null!";
		
		try {
		    this.conn = this.benchmarkModule.makeConnection();
		    this.conn.setAutoCommit(false);
		    conn.setTransactionIsolation(this.wrkld.getIsolationMode());
		} catch (SQLException ex) {
		    throw new RuntimeException("Failed to connect to database", ex);
		}
		
		// Generate all the Procedures that we're going to need
		this.procedures.putAll(this.benchmarkModule.getProcedures());
		assert(this.procedures.size() == this.transactionTypes.size()) :
		    String.format("Failed to get all of the Procedures for %s [expected=%d, actual=%d]",
		                  this.benchmarkModule.getBenchmarkName(),
		                  this.transactionTypes.size(),
		                  this.procedures.size());
        for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
            Procedure proc = e.getValue();
            this.name_procedures.put(e.getKey().getName(), proc);
            this.class_procedures.put(proc.getClass(), proc);
            // e.getValue().generateAllPreparedStatements(this.conn);
        } // FOR
	}
	
	/**
	 * Get the BenchmarkModule managing this Worker
	 */
	@SuppressWarnings("unchecked")
    public final <T extends BenchmarkModule> T getBenchmarkModule() {
	    return ((T)this.benchmarkModule);
	}
	/**
	 * Get the unique thread id for this worker
	 */
	public final int getId() {
		return this.id;
	}
	/**
	 * Get the the total number of workers in this benchmark invocation
	 */
	public final int getNumWorkers() {
	    return (this.benchmarkModule.getWorkloadConfiguration().getTerminals());
	}
	public final WorkloadConfiguration getWorkloadConfiguration() {
	    return (this.benchmarkModule.getWorkloadConfiguration());
	}
	public final Catalog getCatalog() {
	    return (this.benchmarkModule.getCatalog());
	}
	public final Random rng() {
	    return (this.benchmarkModule.rng());
	}
	
	public final Connection getConnection() {
	    return (this.conn);
	}
	
	public final int getRequests() {
        return latencies.size();
    }
	
    public final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }
    
    public final Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return latencies;
    }
    
    /**
     * Returns the current txn count on this worker.
     * Before return the value, the counter is set to 0. This avoids storing past counter values in the client balancer.
     * @return 
     */
    public int getTxncount(){
        int txn = txncount.get();
        txncount.set(0);
        return txn;
    }
	
	public final Procedure getProcedure(TransactionType type) {
        return (this.procedures.get(type));
    }
	@Deprecated
    public final Procedure getProcedure(String name) {
        return (this.name_procedures.get(name));
    }
    @SuppressWarnings("unchecked")
    public final <T extends Procedure> T getProcedure(Class<T> procClass) {
        return (T)(this.class_procedures.get(procClass));
    }
    
    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
    }
    public final Histogram<TransactionType> getTransactionRetryHistogram() {
        return (this.txnRetry);
    }
    public final Histogram<TransactionType> getTransactionAbortHistogram() {
        return (this.txnAbort);
    }
    public final Histogram<TransactionType> getTransactionErrorHistogram() {
        return (this.txnErrors);
    }
    public final Map<TransactionType, Histogram<String>> getTransactionAbortMessageHistogram() {
        return (this.txnAbortMessages);
    }
    
    synchronized public void setCurrStatement(Statement s) {
        this.currStatement = s;
    }

    /**
     * Stop executing the current statement.
     */
    synchronized public void cancelStatement() {
        try {
            if (this.currStatement != null)
                this.currStatement.cancel();
        } catch(SQLException e) {
            LOG.error("Failed to cancel statement: " + e.getMessage());
        }
    }

    /**
     * Get unique name for this worker's thread
     */
    public final String getName() {
        return String.format("worker%03d", this.getId());
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
		assert(invalidTT != null);
		
                work:
		while (true) {

                    // PART 1: Init and check if done

                    preState = wrkldState.getGlobalState();
                    phase = this.wrkldState.getCurrentPhase();

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
                    phase = this.wrkldState.getCurrentPhase();
                    if (phase == null)
                        continue work;

                    //HTPAB
                    pieceOfWork = wrkldState.fetchWork(this);
                    preState = wrkldState.getGlobalState();

                    phase = this.wrkldState.getCurrentPhase();
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
                        type = doWork(preState == State.MEASURE, pieceOfWork, rows);

                        } catch (IndexOutOfBoundsException e) {
                            if (phase.isThroughputRun()) {
                                LOG.error("Thread tried executing disabled phase!");
                                throw e;
                            }
                            if (phase.id == this.wrkldState.getCurrentPhase().id) {
                                switch (preState) {
                                    case WARMUP:
                                    // Don't quit yet: we haven't even begun!
                                        phase.resetSerial();
                                        break;
                                    case COLD_QUERY:
                                    case MEASURE:
                                        // The serial phase is over. Finish the run early.
                                        wrkldState.signalLatencyComplete();
                                        LOG.info("[Serial] Serial execution of all"
                                                 + " transactions complete.");
                                        break;
                                    default:
                                        throw e;
                                }
                            }
                    }

                    // PART 4: Record results

                    long end = System.nanoTime();
                    postState = wrkldState.getGlobalState();

                    switch(postState) {
                        case MEASURE:
                            // Non-serial measurement. Only measure if the state both
                            // before and after was MEASURE, and the phase hasn't
                            // changed, otherwise we're recording results for a query
                            // that either started during the warmup phase or ended
                            // after the timer went off.
                            if (preState == State.MEASURE && type != null
                                && this.wrkldState.getCurrentPhase().id == phase.id) {
                                latencies.addLatency(type.getId(), start, end, this.id, rows.getRows());
                                long latency = end-start;
                                LOG.debug("Stat [Txn ID,latency,rows]: "+type.getId()+", "+latency+", "+rows.getRows());
                                intervalRequests.incrementAndGet();
                            }
                            if (phase.isLatencyRun())
                                this.wrkldState.startColdQuery();
                            break;
                        case COLD_QUERY:
                            // No recording for cold runs, but next time we will since
                            // it'll be a hot run.
                            if (preState == State.COLD_QUERY)
                                this.wrkldState.startHotQuery();
                            break;
                                }

                    wrkldState.finishedWork();
                }

                tearDown(false);
	}

	/**
	 * Called in a loop in the thread to exercise the system under test.
	 * Each implementing worker should return the TransactionType handle that
	 * was executed.
	 */
    protected final TransactionType doWork(boolean measure, SubmittedProcedure pieceOfWork, ResultSetResult rows) {
	    TransactionType next = null;
	    TransactionStatus status = TransactionStatus.RETRY; 
	    Savepoint savepoint = null;
	    final DatabaseType dbType = wrkld.getDBType();
	    final boolean recordAbortMessages = wrkld.getRecordAbortMessages();
	    
	    try {
    	    while (status == TransactionStatus.RETRY && this.wrkldState.getGlobalState() != State.DONE) {
                if (next == null) {
                    next = transactionTypes.getType(pieceOfWork.getType());
                }
    	        assert(next.isSupplemental() == false) :
    	            "Trying to select a supplemental transaction " + next;
    	        
        	    try {
        	        // For Postgres, we have to create a savepoint in order
        	        // to rollback a user aborted transaction
//        	        if (dbType == DatabaseType.POSTGRES) {
//        	            savepoint = this.conn.setSavepoint();
//        	            // if (LOG.isDebugEnabled())
//        	            LOG.info("Created SavePoint: " + savepoint);
//        	        }
        	        
        	        status = this.executeWork(next,rows);
    	        
        	    } catch (UserAbortException ex) {
                    if (LOG.isDebugEnabled()) LOG.debug(next + " Aborted", ex);
                    
                    if (recordAbortMessages) {
                        Histogram<String> error_h = this.txnAbortMessages.get(next);
                        if (error_h == null) {
                            error_h = new Histogram<String>();
                            this.txnAbortMessages.put(next, error_h);
                        }
                        error_h.put(StringUtil.abbrv(ex.getMessage(), 20));
                    }
                    
                    if (savepoint != null) {
                        this.conn.rollback(savepoint);
                    } else {
                        this.conn.rollback();
                    }
                    this.txnAbort.put(next);
                    break;
                    
                // Database System Specific Exception Handling
                } catch (SQLException ex) {
                                       
                    //TODO: Handle acceptable error codes for every DBMS     
                    LOG.debug(next+ " " +  ex.getMessage()+" "+ex.getErrorCode()+ " - " +ex.getSQLState());

                    this.txnErrors.put(next);
                    
                    if (savepoint != null) {
                        this.conn.rollback(savepoint);
                    } else {
                        this.conn.rollback();
                    }

                    if (ex.getSQLState() == null) {
                        continue;
                    }
                    else if (ex.getErrorCode() == 1213 && ex.getSQLState().equals("40001")) {
                        // MySQLTransactionRollbackException
                        continue;
                    } 
                    else if (ex.getErrorCode() == 1205 && ex.getSQLState().equals("41000")) {
                        // MySQL Lock timeout
                        continue;
                    } 
                    else if (ex.getErrorCode() == 1205 && ex.getSQLState().equals("40001")) {
                        // SQLServerException Deadlock
                        continue;
                    }
                    else if (ex.getErrorCode() == -911 && ex.getSQLState().equals("40001")) {
                        // DB2Exception Deadlock
                        continue;
                    } 
                    else if (ex.getErrorCode() == 0 && ex.getSQLState() != null && ex.getSQLState().equals("40001")) {
                        // Postgres serialization
                        continue;
                    } 
                    else if (ex.getErrorCode() == 8177 && ex.getSQLState().equals("72000")) {
                        // ORA-08177: Oracle Serialization
                        continue;
                    } 
                    else if (   (ex.getErrorCode() == 0 && ex.getSQLState().equals("57014"))
                        || (ex.getErrorCode() == -952 && ex.getSQLState().equals("57014")) // DB2
                       )
                    {
                        // Query cancelled by benchmark because we changed
                        // state. That's fine! We expected/caused this.
                        status = TransactionStatus.RETRY_DIFFERENT;
                        continue;
                    }
                    else if (ex.getErrorCode() == 0 && ex.getSQLState().equals("02000")) {
                        // No results returned. That's okay, we can proceed to
                        // a different query. But we should send out a warning,
                        // too, since this is unusual.
                        status = TransactionStatus.RETRY_DIFFERENT;
                        continue;
                    }
                    else {
                        // UNKNOWN: In this case .. Retry as well!
                        continue;
                        //FIXME Disable this for now
                        // throw ex;
                    }
                }
                finally {
                    switch (status) {
                        case SUCCESS:
                            this.txnSuccess.put(next);
                            txncount.incrementAndGet();
                            //LOG.debug("Executed a new invocation of " + next);
                            LOG.info("Executed a new invocation of " + next);
                            break;
                        case RETRY_DIFFERENT:
                            this.txnRetry.put(next);
                            txncount.incrementAndGet();
                            return null;
                        case RETRY:
                            LOG.debug("Retrying transaction...");
                            txncount.incrementAndGet();
                            continue;
                        default:
                            assert(false) :
                                String.format("Unexpected status '%s' for %s", status, next);
                    } // SWITCH
                }

    	    } // WHILE
	    } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error in %s when executing %s [%s]",
                                                     this.getName(), next, dbType), ex);
        } 
        
        return (next);
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
     * @param txnType
     * @return TODO
     * @throws UserAbortException TODO
     * @throws SQLException TODO
     */
	protected abstract TransactionStatus executeWork(TransactionType txnType, ResultSetResult rows) throws UserAbortException, SQLException;
	
	/**
	 * Called at the end of the test to do any clean up that may be
	 * required.
	 * @param error TODO
	 */
	public void tearDown(boolean error) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.warn("No connection to close");
		}
	}

	public void initializeState() {
	    assert (this.wrkldState == null);
	    this.wrkldState = this.wrkld.getWorkloadState();
	}
}
