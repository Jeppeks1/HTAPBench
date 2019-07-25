
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
import java.sql.Timestamp;

/**
 * The business question of Q10 can be expressed as:
 *
 * Identify the customers who have placed an order, which was not fully
 * supplied by the local warehouse and determine the sum of payments received
 * from such orders. Only the 20 first entries are retrieved.
 */
public class Q10 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock){
        int year = RandomParameters.randBetween(1993, 1995);

        int month;
        if (year == 1993)
            month = RandomParameters.randBetween(3, 12);
        else if (year == 1995)
            month = RandomParameters.randBetween(1, 10);
        else
            month = RandomParameters.randBetween(1, 12);

        long date1 = RandomParameters.convertDateToLong(year, month, 1);
        long date2 = RandomParameters.addMonthsToDate(date1, 3);

        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));

        String query = "SELECT c_id, "
                +             "c_first, "
                +             "c_balance, "
                +             "n_name, "
                +             "c_phone, "
                +             "c_data, "
                +             "sum(ol_amount) AS revenue "
                +      "FROM "
                +      HTAPBConstants.TABLENAME_CUSTOMER + ", "
                +      HTAPBConstants.TABLENAME_ORDER + ", "
                +      HTAPBConstants.TABLENAME_ORDERLINE + ", "
                +      HTAPBConstants.TABLENAME_DISTRICT + ", "
                +      HTAPBConstants.TABLENAME_NATION + " "
                +      "WHERE c_w_id = o_w_id "
                +        "AND c_d_id = o_d_id "
                +        "AND c_id   = o_c_id "
                +        "AND o_w_id = ol_w_id "
                +        "AND o_d_id = ol_d_id "
                +        "AND o_id   = ol_o_id "
                +        "AND c_w_id  = d_w_id "
                +        "AND c_d_id  = d_id "
                +        "AND d_nationkey = n_nationkey "
                +        "AND o_entry_d >= '" + ts1.toString() + "' "
                +        "AND o_entry_d < '" + ts2.toString() + "' "
                +        "AND o_all_local = 0 "
                +      "GROUP BY c_id, "
                +               "c_first, "
                +               "c_balance, "
                +               "n_name, "
                +               "c_phone, "
                +               "c_data "
                +      "ORDER BY revenue DESC "
                +      "LIMIT 20";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {

        return buildQueryStmt(clock);
    }
}