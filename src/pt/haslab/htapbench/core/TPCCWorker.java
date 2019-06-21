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

/*
 * jTPCCTerminal - Terminal emulator code for jTPCC (transactions)
 *
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */


import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.procedures.tpcc.TPCCProcedure;
import pt.haslab.htapbench.random.RandomParameters;
import pt.haslab.htapbench.types.ResultSetResult;
import pt.haslab.htapbench.types.TransactionStatus;
import pt.haslab.htapbench.api.Procedure.UserAbortException;
import pt.haslab.htapbench.api.TransactionType;

public class TPCCWorker extends Worker {

    private static final AtomicInteger terminalId = new AtomicInteger(0);
    private AtomicInteger ts_counter;

    private final int terminalWarehouseID;
    private final int terminalDistrictLowerID; // Forms a range [lower, upper] (inclusive).
    private final int terminalDistrictUpperID; // Forms a range [lower, upper] (inclusive).
    private int numWarehouses;

    private final Random rand = new Random();
    private Clock clock;

    private long thinkTime = 0;

    public TPCCWorker(int terminalWarehouseID, int terminalDistrictLowerID, int terminalDistrictUpperID,
                      int numWarehouses, HTAPBenchmark benchmarkModule, AtomicInteger ts_counter, Clock clock) {
        super(benchmarkModule, terminalId.getAndIncrement());

        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictLowerID = terminalDistrictLowerID;
        this.terminalDistrictUpperID = terminalDistrictUpperID;
        this.numWarehouses = numWarehouses;
        this.ts_counter = ts_counter;
        this.clock = clock;

        assert this.terminalDistrictLowerID >= 1;
        assert this.terminalDistrictUpperID <= HTAPBConstants.configDistPerWhse;
        assert this.terminalDistrictLowerID <= this.terminalDistrictUpperID;
    }

    /**
     * Executes a single TPCC transaction of type transactionType.
     */
    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction, ResultSetResult rows) throws UserAbortException, SQLException {
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, rand, terminalWarehouseID, numWarehouses, terminalDistrictLowerID, terminalDistrictUpperID, this);
            setThinkTime(thinkTime() + proc.getKeyingTime());

            //waits the required ThinkTime per txn.
            Thread.sleep(getThinkTime());
        } catch (ClassCastException ex) {
            //fail gracefully
            System.err.println("TPC-C: We have been invoked with an INVALID transactionType?!");
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        } catch (RuntimeException ex) {
            conn.rollback();
            System.err.println("TPC-C txn will restart " + ex.getMessage());
            return (TransactionStatus.ABORTED);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(TPCCWorker.class.getName()).log(Level.SEVERE, null, ex);
        }

        conn.commit();

        return (TransactionStatus.SUCCESS);
    }

    public AtomicInteger getTsCounter() {
        return this.ts_counter;
    }

    public WorkloadConfiguration getWrkld() {
        return wrkld;
    }

    private long thinkTime() {
        return (RandomParameters.negExp(rand, getThinkTime(), 4.54e-5, getThinkTime()));
    }

    private long getThinkTime() {
        return this.thinkTime;
    }

    private void setThinkTime(long thinkTime) {
        this.thinkTime = thinkTime;
    }

    public Clock getClock() {
        return this.clock;
    }

}

