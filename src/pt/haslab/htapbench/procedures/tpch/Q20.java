
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
 * The business question of Q20 can be expressed as:
 *
 * Find the suppliers who have supplied warehouses, that now have an excess of a given
 * item available in stock.
 */
public class Q20 extends GenericQuery {

    private SQLStmt buildQueryStmt(Clock clock){

        String nation = random.getRandomNation();
        String char1 = random.generateRandomCharacter() + "%";

        int year = RandomParameters.randBetween(1993, 1997);
        int month = RandomParameters.randBetween(1, 12);
        long date1 = RandomParameters.convertDateToLong(year, month, 1);
        long date2 = RandomParameters.convertDateToLong(year + 1, month, 1);

        Timestamp ts1 = new Timestamp(clock.transformDeliveryTsToLong(date1));
        Timestamp ts2 = new Timestamp(clock.transformDeliveryTsToLong(date2));

        String query = "SELECT n_name, "
                +             "su_address "
                +      "FROM "
                +      HTAPBConstants.TABLENAME_ORDERLINE + ", "
                +      HTAPBConstants.TABLENAME_STOCK + ", "
                +      HTAPBConstants.TABLENAME_ITEM + ", "
                +      HTAPBConstants.TABLENAME_SUPPLIER + ", "
                +      HTAPBConstants.TABLENAME_NATION + " "
                +      "WHERE ol_supply_w_id = s_w_id "
                +        "AND ol_i_id = s_i_id "
                +        "AND s_i_id = i_id "
                +        "AND s_suppkey = su_suppkey "
                +        "AND su_nationkey = n_nationkey "
                +        "AND n_name = '" + nation + "' "
                +        "AND ol_delivery_d >= '" + ts1.toString() + "' "
                +        "AND ol_delivery_d < '" + ts2.toString() + "' "
                +        "AND i_data LIKE '" + char1 + "' "
                +        "AND s_quantity > ( "
                +                    "SELECT avg(s_quantity) "
                +                    "FROM "
                +                     HTAPBConstants.TABLENAME_ORDERLINE + ", "
                +                     HTAPBConstants.TABLENAME_STOCK + " "
                +                    "WHERE ol_supply_w_id = s_w_id "
                +                      "AND ol_i_id = s_i_id "
                +                      "AND ol_delivery_d >= '" + ts1.toString() + "' "
                +                      "AND ol_delivery_d < '" + ts2.toString() + "') "
                +      "ORDER BY su_name, su_address";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt(clock);
    }
}