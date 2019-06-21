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
import java.util.HashMap;
import java.util.Map;

public class HTAPBConstants {

    public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // -------------------------------------------------------------------
    //                           Table names
    // -------------------------------------------------------------------

    public static final String TABLENAME_WAREHOUSE = "WAREHOUSE";
    public static final String TABLENAME_ORDERLINE = "ORDER_LINE";
    public static final String TABLENAME_NEWORDER = "NEW_ORDER";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_DISTRICT = "DISTRICT";
    public static final String TABLENAME_SUPPLIER = "SUPPLIER";
    public static final String TABLENAME_HISTORY = "HISTORY";
    public static final String TABLENAME_NATION = "NATION";
    public static final String TABLENAME_REGION = "REGION";
    public static final String TABLENAME_STOCK = "STOCK";
    public static final String TABLENAME_ORDER = "OORDER";
    public static final String TABLENAME_ITEM = "ITEM";

    // -------------------------------------------------------------------
    //                           CSV filenames
    // -------------------------------------------------------------------

    public static final String CSVNAME_WAREHOUSE = "warehouse.csv";
    public static final String CSVNAME_ORDERLINE = "order_line.csv";
    public static final String CSVNAME_NEWORDER = "new_order.csv";
    public static final String CSVNAME_CUSTOMER = "customer.csv";
    public static final String CSVNAME_DISTRICT = "district.csv";
    public static final String CSVNAME_SUPPLIER = "supplier.csv";
    public static final String CSVNAME_HISTORY = "history.csv";
    public static final String CSVNAME_REGION = "region.csv";
    public static final String CSVNAME_NATION = "nation.csv";
    public static final String CSVNAME_STOCK = "stock.csv";
    public static final String CSVNAME_ORDER = "oorder.csv";
    public static final String CSVNAME_ITEM = "item.csv";

    // -------------------------------------------------------------------
    //                           Iterable map
    // -------------------------------------------------------------------

    public static Map<String, String> tableToCSV = new HashMap<String, String>();
    static {
        tableToCSV.put(TABLENAME_WAREHOUSE, CSVNAME_WAREHOUSE);
        tableToCSV.put(TABLENAME_ORDERLINE, CSVNAME_ORDERLINE);
        tableToCSV.put(TABLENAME_NEWORDER, CSVNAME_NEWORDER);
        tableToCSV.put(TABLENAME_CUSTOMER, CSVNAME_CUSTOMER);
        tableToCSV.put(TABLENAME_DISTRICT, CSVNAME_DISTRICT);
        tableToCSV.put(TABLENAME_SUPPLIER, CSVNAME_SUPPLIER);
        tableToCSV.put(TABLENAME_HISTORY, CSVNAME_HISTORY);
        tableToCSV.put(TABLENAME_REGION, CSVNAME_REGION);
        tableToCSV.put(TABLENAME_NATION, CSVNAME_NATION);
        tableToCSV.put(TABLENAME_STOCK, CSVNAME_STOCK);
        tableToCSV.put(TABLENAME_ORDER, CSVNAME_ORDER);
        tableToCSV.put(TABLENAME_ITEM, CSVNAME_ITEM);
    }

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
    static final double max_tmpC = 1.281;
}
