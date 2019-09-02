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
 * The business question of Q8 can be expressed as:
 *
 * Find the market share for a given nation within a specified date range.
 */
public class Q8 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock){

        String region = random.getRandomRegion();
        String nation = random.getRandomNation(region);

        String i_data = "%" + random.generateRandomCharacter();

        long date1 = RandomParameters.convertDateToLong(1995, 1, 1);
        long date2 = RandomParameters.convertDateToLong(1996, 12, 31);

        Timestamp ts1 = new Timestamp(clock.transformOrderTsToLong(date1));
        Timestamp ts2 = new Timestamp(clock.transformOrderTsToLong(date2));

        String query = "SELECT sum(CASE WHEN n2.n_name = '" + nation + "' "
                +                 "THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share "
                +      "FROM "
                +      HTAPBConstants.TABLENAME_CUSTOMER + ", "
                +      HTAPBConstants.TABLENAME_ORDER + ", "
                +      HTAPBConstants.TABLENAME_ORDERLINE +", "
                +      HTAPBConstants.TABLENAME_STOCK + ", "
                +      HTAPBConstants.TABLENAME_ITEM + ", "
                +      HTAPBConstants.TABLENAME_DISTRICT + ", "
                +      HTAPBConstants.TABLENAME_SUPPLIER + ", "
                +      HTAPBConstants.TABLENAME_NATION + " n1, "
                +      HTAPBConstants.TABLENAME_NATION + " n2, "
                +      HTAPBConstants.TABLENAME_REGION + " "
                +      "WHERE c_w_id = o_w_id "
                +        "AND c_d_id = o_d_id "
                +        "AND c_id   = o_c_id "
                +        "AND o_w_id = ol_w_id "
                +        "AND o_d_id = ol_d_id "
                +        "AND o_id   = ol_o_id "
                +        "AND ol_supply_w_id = s_w_id "
                +        "AND ol_i_id = s_i_id "
                +        "AND c_w_id  = d_w_id "
                +        "AND c_d_id  = d_id "
                +        "AND s_i_id  = i_id "
                +        "AND s_suppkey = su_suppkey "
                +        "AND d_nationkey = n1.n_nationkey "
                +        "AND su_nationkey = n2.n_nationkey "
                +        "AND n1.n_regionkey = r_regionkey "
                +        "AND r_name = '" + region + "' "
                +        "AND o_entry_d between '" + ts1.toString() + "' AND '" + ts2.toString() + "' "
                +        "AND i_data LIKE '" + i_data + "'";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}