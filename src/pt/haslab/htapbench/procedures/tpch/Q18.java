
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
package pt.haslab.htapbench.procedures.tpch;

import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.api.SQLStmt;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.core.Clock;
import pt.haslab.htapbench.random.RandomParameters;

/**
 * The business question of Q18 can be expressed as:
 *
 * Determine the revenue from large quantity orders for the top 100 customers.
 */
public class Q18 extends GenericQuery {

    private SQLStmt buildQueryStmt(){
        String amount = "" + RandomParameters.randBetween(12, 15);

        String query = "SELECT c_last, "
                +             "c_id, "
                +             "o_id, "
                +             "o_entry_d, "
                +             "o_ol_cnt, "
                +             "sum(ol_amount) AS amount_sum "
                +      "FROM "
                +      HTAPBConstants.TABLENAME_CUSTOMER + ", "
                +      HTAPBConstants.TABLENAME_ORDER +    ", "
                +      HTAPBConstants.TABLENAME_ORDERLINE
                +      " WHERE c_w_id = o_w_id "
                +         "AND c_d_id = o_d_id "
                +         "AND c_id   = o_c_id "
                +         "AND o_w_id = ol_w_id "
                +         "AND o_d_id = ol_d_id "
                +         "AND o_id   = ol_o_id "
                +         "AND ol_amount > " + amount + " "
                +      "GROUP BY c_last, "
                +               "c_id, "
                +               "o_id, "
                +               "o_entry_d, "
                +               "o_ol_cnt "
                +      "ORDER BY amount_sum DESC, o_entry_d "
                +      "LIMIT 100";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}