
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
 * The business question of Q21 can be expressed as:
 *
 * Determine the number of customers waiting for an order to be delivered,
 * by customer country and carrier id.
 */
public class Q21 extends GenericQuery {

    private SQLStmt buildQueryStmt(){

        String nation = random.getRandomNation();

        String query = "SELECT n_name, "
            +                 "o_carrier_id, "
            +                 "count(*) AS numwait "
            +          "FROM "
            +          HTAPBConstants.TABLENAME_CUSTOMER + ", "
            +          HTAPBConstants.TABLENAME_ORDER + ", "
            +          HTAPBConstants.TABLENAME_ORDERLINE + ", "
            +          HTAPBConstants.TABLENAME_DISTRICT + ", "
            +          HTAPBConstants.TABLENAME_NATION + " "
            +          "WHERE c_w_id = o_w_id "
            +            "AND c_d_id = o_d_id "
            +            "AND c_id   = o_c_id "
            +            "AND o_w_id = ol_w_id "
            +            "AND o_d_id = ol_d_id "
            +            "AND o_id   = ol_o_id "
            +            "AND c_w_id = d_w_id "
            +            "AND c_d_id = d_id "
            +            "AND d_nationkey = n_nationkey "
            +            "AND ol_delivery_d > o_entry_d "
            +            "AND n_name = '" + nation + "' "
            +          "GROUP BY n_name, "
            +                   "o_carrier_id "
            +          "ORDER BY numwait DESC, "
            +                   "n_name, "
            +                   "o_carrier_id";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}