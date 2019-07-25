
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
 * The business question of Q17 can be expressed as:
 *
 * Determine how much revenue would be lost if orders were no longer filled for small
 * quantities of certain parts.
 */
public class Q17 extends GenericQuery {

    private SQLStmt buildQueryStmt(){

        String i_data = "%" + random.generateRandomCharacter();

        String query = "SELECT SUM(ol_amount) / 2.0 AS avg_yearly "
            +          "FROM "
            +           HTAPBConstants.TABLENAME_ORDERLINE + ", "
            +           HTAPBConstants.TABLENAME_ITEM + " "
            +          "WHERE ol_i_id = i_id "
            +            "AND i_data LIKE '" + i_data + "' "
            +            "AND ol_quantity < "
            +                  "(SELECT AVG(i_price) * 0.2 AS avg_price "
            +                   "FROM " + HTAPBConstants.TABLENAME_ITEM + " "
            +                   "WHERE i_id = ol_i_id "
            +                     "AND i_data LIKE '" + i_data + "' "
            +                   "GROUP BY i_id, i_price)";
       return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}