
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

public class Q1 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock) {
        // Compute random number of days [60,120]
        int days = RandomParameters.randBetween(60, 120);

        // Calculate the correct offset and then convert to the correct timestamp
        long ts_minusXdays = clock.computeEndMinusXDays(days);
        Timestamp ts = new Timestamp(clock.transformTsFromSpecToLong(ts_minusXdays));

        String query = "SELECT ol_number, "
                + "sum(ol_quantity) AS sum_qty, "
                + "sum(ol_amount) AS sum_amount, "
                + "avg(ol_quantity) AS avg_qty, "
                + "avg(ol_amount) AS avg_amount, "
                + "count(*) AS count_order "
                + "FROM " + HTAPBConstants.TABLENAME_ORDERLINE
                + " WHERE ol_delivery_d <= '"
                + ts.toString()
                + "' GROUP BY ol_number "
                + "ORDER BY ol_number";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }


}