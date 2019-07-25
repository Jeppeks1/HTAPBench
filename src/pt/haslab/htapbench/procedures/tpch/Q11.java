
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
import java.text.DecimalFormat;

/**
 * The business question of Q11 can be expressed as:
 *
 * Find the most valuable item assets in stock supplied from a given nation,
 * where the value is defined as the number of items in stock times the price
 * of that item. Only assets above a certain threshold is reported on.
 */
public class Q11 extends GenericQuery {

    private SQLStmt buildQueryStmt(){

        String nation = random.getRandomNation();

        double frac = RandomParameters.randDoubleBetween(0, 5) / 100000;
        String fraction = new DecimalFormat("#.###########").format(frac).replace(',', '.');

        String query = "SELECT s_i_id, "
                +             "sum(i_price * s_quantity) AS value "
                +      "FROM "
                +      HTAPBConstants.TABLENAME_ITEM + ", "
                +      HTAPBConstants.TABLENAME_STOCK + ", "
                +      HTAPBConstants.TABLENAME_SUPPLIER + ", "
                +      HTAPBConstants.TABLENAME_NATION + " "
                +      "WHERE i_id = s_i_id "
                +        "AND s_suppkey = su_suppkey "
                +        "AND su_nationkey = n_nationkey "
                +        "AND n_name = '" + nation + "' "
                +      "GROUP BY s_i_id "
                +      "HAVING sum(i_price * s_quantity) > ( "
                +              "SELECT sum(i_price * s_quantity) * " + fraction + " "
                +              "FROM "
                +               HTAPBConstants.TABLENAME_ITEM + ", "
                +               HTAPBConstants.TABLENAME_STOCK + ", "
                +               HTAPBConstants.TABLENAME_SUPPLIER + ", "
                +               HTAPBConstants.TABLENAME_NATION + " "
                +              "WHERE i_id = s_i_id "
                +                "AND s_suppkey = su_suppkey "
                +                "AND su_nationkey = n_nationkey "
                +                "AND n_name = '" + nation + "') "
                +      "ORDER BY value DESC";

        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}