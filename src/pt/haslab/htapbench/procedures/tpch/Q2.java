
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
 * The business question of Q2 can be expressed as:
 *
 * Find the supplier of items with a globally minimum stock quantity, for a given region.
 */
public class Q2 extends GenericQuery {

    private SQLStmt buildQueryStmt(){

        String region = random.getRandomRegion();

        String i_data = "%" + random.generateRandomCharacter();

        String query ="SELECT su_acctbal, "
            +                "su_name, "
            +                "n_name, "
            +                "i_name, "
            +                "su_address, "
            +                "su_phone, "
            +                "su_comment "
            +          "FROM "
            +          HTAPBConstants.TABLENAME_ITEM + ", "
            +          HTAPBConstants.TABLENAME_STOCK + ", "
            +          HTAPBConstants.TABLENAME_SUPPLIER + ", "
            +          HTAPBConstants.TABLENAME_NATION + ", "
            +          HTAPBConstants.TABLENAME_REGION + " "
            +          "WHERE i_id = s_i_id "
            +            "AND s_suppkey = su_suppkey "
            +            "AND su_nationkey = n_nationkey "
            +            "AND n_regionkey = r_regionkey "
            +            "AND i_data LIKE '" + i_data + "' "
            +            "AND r_name = '" + region + "' "
            +            "AND s_quantity = "
            +                   "(SELECT min(s_quantity) "
            +                    "FROM "
            +                     HTAPBConstants.TABLENAME_ITEM + ", "
            +                     HTAPBConstants.TABLENAME_STOCK + ", "
            +                     HTAPBConstants.TABLENAME_SUPPLIER + ", "
            +                     HTAPBConstants.TABLENAME_NATION + ", "
            +                     HTAPBConstants.TABLENAME_REGION + " "
            +                    "WHERE i_id = s_i_id "
            +                      "AND s_suppkey = su_suppkey "
            +                      "AND su_nationkey = n_nationkey "
            +                      "AND n_regionkey = r_regionkey "
            +                      "AND i_data LIKE '" + i_data + "' "
            +                      "AND r_name LIKE '" + region + "') "
            +          "ORDER BY su_acctbal DESC, "
            +                   "n_name, "
            +                   "su_name, "
            +                   "i_name "
            +          "LIMIT 100";
        return new SQLStmt(query);

    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}