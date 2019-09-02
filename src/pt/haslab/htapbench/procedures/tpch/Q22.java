
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
 * The business question of Q21 can be expressed as:
 *
 * Find the customers grouped by nation that have a greater than average
 * account balance, within a specific set of phone-country codes.
 */
public class Q22 extends GenericQuery {

    private SQLStmt buildQueryStmt(){

        String code1 = random.getRandomPhoneCountryCode();
        String code2 = random.getRandomPhoneCountryCode();
        String code3 = random.getRandomPhoneCountryCode();
        String code4 = random.getRandomPhoneCountryCode();
        String code5 = random.getRandomPhoneCountryCode();
        String code6 = random.getRandomPhoneCountryCode();
        String code7 = random.getRandomPhoneCountryCode();

        String query = "SELECT n_name, "
                +             "count(*) AS numcust, "
                +             "sum(c_balance) AS totacctbal "
                +      "FROM "
                +       HTAPBConstants.TABLENAME_CUSTOMER + ", "
                +       HTAPBConstants.TABLENAME_DISTRICT + ", "
                +       HTAPBConstants.TABLENAME_NATION + " "
                +      "WHERE c_w_id = d_w_id "
                +        "AND c_d_id = d_id "
                +        "AND d_nationkey = n_nationkey "
                +        "AND substring(c_phone from 1 for 2) IN "
                +                     "('" + code1 + "', "
                +                      "'" + code2 + "', "
                +                      "'" + code3 + "', "
                +                      "'" + code4 + "', "
                +                      "'" + code5 + "', "
                +                      "'" + code6 + "', "
                +                      "'" + code7 + "') "
                +        "AND c_balance > "
                +          "(SELECT avg(c_balance) "
                +           "FROM " + HTAPBConstants.TABLENAME_CUSTOMER + " "
                +           "WHERE c_balance > 0.00 "
                +             "AND substring(c_phone from 1 for 2) IN "
                +                          "('" + code1 + "',"
                +                           "'" + code2 + "',"
                +                           "'" + code3 + "',"
                +                           "'" + code4 + "',"
                +                           "'" + code5 + "',"
                +                           "'" + code6 + "',"
                +                           "'" + code7 + "')) "
                +      "GROUP BY n_name "
                +      "ORDER BY n_name ";
        return new SQLStmt(query);
    }

    @Override
    protected SQLStmt get_query(Clock clock, WorkloadConfiguration wrklConf) {
        return buildQueryStmt();
    }
}