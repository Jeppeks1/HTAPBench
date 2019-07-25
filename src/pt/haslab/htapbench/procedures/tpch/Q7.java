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
 * The business question of Q7 can be expressed as:
 *
 * Determine the total revenue of orders placed by a customer in one nation,
 * but supplied by a supplier in another nation with a delivery date in a
 * given range.
 */
public class Q7 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock){

        // The nation should be within the same region
        String region = random.getRandomRegion();
        String nation1 = random.getRandomNation(region);
        String nation2 = random.getRandomNation(region);

        // Retry until the nations are different
        if (nation1.equals(nation2))
            buildQueryStmt(clock);

        long date1 = RandomParameters.convertDateToLong(1995, 1, 1);
        long date2 = RandomParameters.convertDateToLong(1996, 12, 31);

        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));

        String query = "SELECT n1.n_name as su_nation, "
                +             "n2.n_name as c_nation, "
                +             "sum(ol_amount) AS revenue "
                +      "FROM "
                +      HTAPBConstants.TABLENAME_CUSTOMER + ", "
                +      HTAPBConstants.TABLENAME_ORDER +    ", "
                +      HTAPBConstants.TABLENAME_ORDERLINE+ ", "
                +      HTAPBConstants.TABLENAME_STOCK +    ", "
                +      HTAPBConstants.TABLENAME_DISTRICT + ", "
                +      HTAPBConstants.TABLENAME_SUPPLIER + ", "
                +      HTAPBConstants.TABLENAME_NATION   + " n1, "
                +      HTAPBConstants.TABLENAME_NATION   + " n2 "
                +      "WHERE c_w_id = o_w_id "
                +        "AND c_d_id = o_d_id "
                +        "AND c_id   = o_c_id "
                +        "AND o_w_id = ol_w_id "
                +        "AND o_d_id = ol_d_id "
                +        "AND o_id   = ol_o_id "
                +        "AND ol_supply_w_id = s_w_id "
                +        "AND ol_i_id   = s_i_id "
                +        "AND c_w_id  = d_w_id "
                +        "AND c_d_id  = d_id "
                +        "AND s_suppkey = su_suppkey "
                +        "AND su_nationkey = n1.n_nationkey "
                +        "AND d_nationkey  = n2.n_nationkey "
                +        "AND ol_delivery_d between '" + ts1.toString() + "' and '" + ts2.toString() + "' "
                +        "AND ((n1.n_name = '" + nation1 + "' AND n2.n_name = '" + nation2 + "') "
                +             "OR (n1.n_name = '" + nation2 + "' AND n2.n_name = '" + nation1 + "')) "
                +      "GROUP BY su_nationkey, "
                +               "c_nation "
                +      "ORDER BY su_nationkey, "
                +               "c_nation";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}