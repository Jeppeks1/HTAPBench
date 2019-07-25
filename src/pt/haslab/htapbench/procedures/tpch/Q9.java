
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

/**
 * The business question of Q9 can be expressed as:
 *
 * Determine how much profit is made on a range of items, grouped by the
 * supplier nation and order year.
 */
public class Q9 extends GenericQuery {

    private SQLStmt buildQueryStmt(){

        Character st2 = random.generateRandomCharacter();
        Character st1 = random.generateRandomCharacter();

        String i_data = "%" + st1 + st2;

        String query = "SELECT n_name, "
                +             "YEAR(o_entry_d) AS o_year, "
                +             "sum(ol_amount) AS sum_profit "
                +      "FROM "
                +       HTAPBConstants.TABLENAME_ORDER + ", "
                +       HTAPBConstants.TABLENAME_ORDERLINE + ", "
                +       HTAPBConstants.TABLENAME_STOCK + ", "
                +       HTAPBConstants.TABLENAME_ITEM + ", "
                +       HTAPBConstants.TABLENAME_SUPPLIER + ", "
                +       HTAPBConstants.TABLENAME_NATION + " "
                +      "WHERE o_w_id = ol_w_id "
                +        "AND o_d_id = ol_d_id "
                +        "AND o_id   = ol_o_id "
                +        "AND ol_supply_w_id = s_w_id "
                +        "AND ol_i_id = s_i_id "
                +        "AND s_i_id  = i_id "
                +        "AND s_suppkey = su_suppkey "
                +        "AND su_nationkey = n_nationkey "
                +        "AND i_data LIKE '" + i_data + "' "
                +      "GROUP BY n_name, "
                +               "o_year "
                +      "ORDER BY n_name, "
                +               "o_year";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}