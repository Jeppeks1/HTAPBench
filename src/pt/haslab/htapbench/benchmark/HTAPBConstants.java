/*
 * Copyright 2017 by INESC TEC
 * Developed by Fábio Coelho
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
package pt.haslab.htapbench.benchmark;

import java.text.SimpleDateFormat;

public class HTAPBConstants {

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // -------------------------------------------------------------------
    //                           Table names
    // -------------------------------------------------------------------

    public static final String TABLENAME_DISTRICT = "DISTRICT";
    public static final String TABLENAME_WAREHOUSE = "WAREHOUSE";
    public static final String TABLENAME_ITEM = "ITEM";
    public static final String TABLENAME_STOCK = "STOCK";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_HISTORY = "HISTORY";
    public static final String TABLENAME_ORDER = "OORDER";
    public static final String TABLENAME_ORDERLINE = "ORDER_LINE";
    public static final String TABLENAME_NEWORDER = "NEW_ORDER";
    public static final String TABLENAME_NATION = "NATION";
    public static final String TABLENAME_REGION = "REGION";
    public static final String TABLENAME_SUPPLIER = "SUPPLIER";

    // -------------------------------------------------------------------
    //                           TPC-C constants
    // -------------------------------------------------------------------
    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public final static int configCommitCount = 1000; // commit every n records
    public final static int configItemCount = 100000; // tpc-c std = 100,000
    public final static int configDistPerWhse = 10; // tpc-c std = 10
    public final static int configCustPerDist = 3000; // tpc-c std = 3,000

    // An invalid item id used to rollback a new order transaction.
    public static final int INVALID_ITEM_ID = -12345;

    public static final long keyingTime_NewOrder = 18000;
    public static final long keyingTime_Payment = 3000;
    public static final long keyingTime_Delivery = 2000;
    public static final long keyingTime_OrderStatus = 2000;
    public static final long keyingTime_StockLevel = 2000;

    //the max tpmC considered according to TPC-C specification [pag. 61 - clause 4.2].
    public static final double max_tmpC = 1.281;
}
