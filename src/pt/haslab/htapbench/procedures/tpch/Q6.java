
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
 * The business question of Q6 can be expressed as:
 *
 * Find the revenue increase for orders if the customer's discount had been
 * eliminated for a given percentage range in a given year.
 */
public class Q6 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock, boolean hybridWkrld){
        double discount1;
        double discount2;

        double d1 = RandomParameters.randDoubleBetween(2, 9) / 100 - 0.01;
        double d2 = RandomParameters.randDoubleBetween(2, 9) / 100 - 0.01;

        // At least some SQL flavours requires the first expression in the
        // BETWEEN clause to be less than or equal to the second expression,
        // in order to produce a non-null result-set.
        if (d1 > d2){
            discount1 = d2;
            discount2 = d1;
        } else {
            discount1 = d1;
            discount2 = d2;
        }

        int year = RandomParameters.randBetween(1993, 1997);

        long date1 = RandomParameters.convertDateToLong(year, 1, 1);
        long date2 = RandomParameters.convertDateToLong(year + 1, 1, 1);

        Timestamp ts1 = new Timestamp(clock.transformTsFromSpecToLong(date1));
        Timestamp ts2 = new Timestamp(clock.transformTsFromSpecToLong(date2));

        String query = "SELECT sum(ol_amount * c_discount) AS revenue "
                +      "FROM "
                +       HTAPBConstants.TABLENAME_CUSTOMER + ", "
                +       HTAPBConstants.TABLENAME_ORDER + ", "
                +       HTAPBConstants.TABLENAME_ORDERLINE + " "
                +      "WHERE c_w_id = o_w_id "
                +        "AND c_d_id = o_d_id "
                +        "AND c_id   = o_c_id "
                +        "AND o_w_id = ol_w_id "
                +        "AND o_d_id = ol_d_id "
                +        "AND o_id   = ol_o_id "
                +        "AND c_discount BETWEEN " + discount1 + " AND " + discount2 + " "
                +        "AND ol_delivery_d >= '" + ts1 + "' "
                +        "AND ol_delivery_d < '" + ts2 + "' ";

        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock,WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock, wrklConf.getHybridWorkload());
    }
}