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
package pt.haslab.htapbench.benchmark;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.Worker;

import pt.haslab.htapbench.procedures.tpch.GenericQuery.InvalidResultException;
import pt.haslab.htapbench.procedures.tpch.GenericQuery;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.types.ResultSetResult;
import pt.haslab.htapbench.types.TransactionStatus;

public class TPCHWorker extends Worker {

    private static final Logger LOG = Logger.getLogger(TPCHWorker.class);
    private Clock clock;

    TPCHWorker(BenchmarkModule benchmarkModule, Clock clock) {
        super(benchmarkModule, terminalId.getAndIncrement());
        this.clock = clock;
    }

    private static final AtomicInteger terminalId = new AtomicInteger(0);

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction, ResultSetResult rows)
            throws SQLException {
        // Get the procedure to be executed
        GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());

        try {
            proc.setOwner(this);
            int resultSetRowNumber = proc.run(conn, clock, super.getWorkloadConfiguration());
            rows.setRows(resultSetRowNumber);
        } catch (InvalidResultException ex) {
            recordMessage(nextTransaction, ex);
            return TransactionStatus.INVALID_RESULT;
        }

        // TPCH transactions cannot be committed. If they are, it will interfere with the statistics returned.
        return TransactionStatus.SUCCESS;
    }
}

