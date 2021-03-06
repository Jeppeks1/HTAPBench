
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
 * The business question of Q4 can be expressed as:
 *
 * Determine how many orders have been placed and how many items were
 * requested in that order. Filter between a given time period and only
 * for orders that have been delivered.
 */
public class Q4 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock){
        int year = RandomParameters.randBetween(1993, 1997);

        int month;
        if (year == 1997)
            month = RandomParameters.randBetween(1, 10);
        else
            month = RandomParameters.randBetween(1, 12);

        long date1 = RandomParameters.convertDateToLong(year, month, 1);
        long date2 = RandomParameters.addMonthsToDate(date1, 3);

        Timestamp ts1 = new Timestamp(clock.transformOrderTsToLong(date1));
        Timestamp ts2 = new Timestamp(clock.transformOrderTsToLong(date2));

        String query = "SELECT o_ol_cnt, "
                +             "count(*) AS order_count "
                +      "FROM " + HTAPBConstants.TABLENAME_ORDER + " "
                +      "WHERE o_entry_d >= '" + ts1.toString() + "' "
                +        "AND o_entry_d < '" + ts2.toString() + "' "
                +        "AND EXISTS "
                +                "(SELECT * "
                +                 "FROM " + HTAPBConstants.TABLENAME_ORDERLINE + " "
                +                 "WHERE o_id = ol_o_id "
                +                   "AND o_w_id = ol_w_id "
                +                   "AND o_d_id = ol_d_id "
                +                   "AND ol_delivery_d >= o_entry_d) "
                +      "GROUP BY o_ol_cnt "
                +      "ORDER BY o_ol_cnt";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}