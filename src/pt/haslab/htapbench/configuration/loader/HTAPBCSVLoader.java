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
package pt.haslab.htapbench.configuration.loader;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.benchmark.AuxiliarFileHandler;
import pt.haslab.htapbench.configuration.loader.pojo.*;
import pt.haslab.htapbench.core.HTAPBenchmark;
import pt.haslab.htapbench.random.RandomGenerator;
import pt.haslab.htapbench.random.RandomParameters;
import pt.haslab.htapbench.util.TPCCUtil;

import java.io.*;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import static pt.haslab.htapbench.benchmark.HTAPBConstants.*;


/**
 * HTAPB database Loader.
 *
 * @author Fábio Coelho <fabio.a.coelho@inesctec.pt>
 */

public class HTAPBCSVLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(HTAPBCSVLoader.class);

    public HTAPBCSVLoader(HTAPBenchmark benchmark) {
        super(benchmark, null);

        numWarehouses = (int) Math.round(this.scaleFactor);

        if (numWarehouses == 0)
            numWarehouses = 1;

        counter = new AtomicInteger();
    }


    // ********** general vars **********************************
    private static Date now = null;

    private static Random gen;
    private static int numWarehouses = 0;
    private static PrintWriter out = null;
    private static long lastTimeMS = 0;

    private static final RandomGenerator ran = new RandomGenerator(0);
    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    private AtomicInteger counter;

    private static final int[] nationkeys = new int[62];

    static {
        for (char i = 0; i < 10; i++) {
            nationkeys[i] = '0' + i;
        }
        for (char i = 0; i < 26; i++) {
            nationkeys[i + 10] = 'A' + i;
        }
        for (char i = 0; i < 26; i++) {
            nationkeys[i + 36] = 'a' + i;
        }
    }

    private int loadItem(int itemKount) {

        int k = 0;
        int t;
        int randPct;
        int len;
        int startORIGINAL;

        try {
            Item item = new Item();
            now = new java.util.Date();
            out = new PrintWriter(new FileOutputStream(itemPath));
            t = itemKount;

            LOG.debug("\nStart Item Load for " + t + " Items @ " + now + " ...");
            LOG.debug("\nWriting Item file to: " + itemPath);

            for (int i = 1; i <= itemKount; i++) {

                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, gen));
                item.i_price = TPCCUtil.randomNumber(100, 10000, gen) / 100.0;

                // i_data
                randPct = TPCCUtil.randomNumber(1, 100, gen);
                len = TPCCUtil.randomNumber(26, 50, gen);
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
                    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL"
                            + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtil.randomNumber(1, 10000, gen);

                k++;
                String str = "";
                str = str + item.i_id + ",";
                str = str + item.i_name + ",";
                str = str + item.i_price + ",";
                str = str + item.i_data + ",";
                str = str + item.i_im_id;
                out.println(str);

                if ((k % configCommitCount) == 0) {
                    long tmpTime = new java.util.Date().getTime();
                    String etStr = "  Elasped Time(ms): "
                            + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";
                    LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
                    lastTimeMS = tmpTime;
                }
            } // end for

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
            LOG.debug("End Item Load @  " + now);

            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return k;

    } // end loadItem()

    private int loadStock(int whseKount, int itemKount) {

        int k = 0;
        int t;
        int randPct;
        int len;
        int startORIGINAL;

        try {
            now = new java.util.Date();
            t = (whseKount * itemKount);

            out = new PrintWriter(new FileOutputStream(stockPath));

            LOG.debug("\nStart Stock Load for " + t + " units @ " + now + " ...");
            LOG.debug("\nWriting Stock file to: " + stockPath);

            Stock stock = new Stock();

            for (int i = 1; i <= itemKount; i++) {
                for (int w = 1; w <= whseKount; w++) {
                    stock.s_i_id = i;
                    stock.s_w_id = w;
                    stock.s_quantity = TPCCUtil.randomNumber(10, 100, gen);
                    stock.s_suppkey = TPCCUtil.randomNumber(1, 10000, gen);
                    stock.s_ytd = 0;
                    stock.s_order_cnt = 0;
                    stock.s_remote_cnt = 0;

                    // s_data
                    randPct = TPCCUtil.randomNumber(1, 100, gen);
                    len = TPCCUtil.randomNumber(26, 50, gen);
                    if (randPct > 10) {
                        // 90% of time i_data isa random string of length [26 .. 50]
                        stock.s_data = TPCCUtil.randomStr(len);
                    } else {
                        // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
                        startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
                        stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL"
                                + TPCCUtil.randomStr(len - startORIGINAL - 9);
                    }

                    stock.s_dist_01 = TPCCUtil.randomStr(24);
                    stock.s_dist_02 = TPCCUtil.randomStr(24);
                    stock.s_dist_03 = TPCCUtil.randomStr(24);
                    stock.s_dist_04 = TPCCUtil.randomStr(24);
                    stock.s_dist_05 = TPCCUtil.randomStr(24);
                    stock.s_dist_06 = TPCCUtil.randomStr(24);
                    stock.s_dist_07 = TPCCUtil.randomStr(24);
                    stock.s_dist_08 = TPCCUtil.randomStr(24);
                    stock.s_dist_09 = TPCCUtil.randomStr(24);
                    stock.s_dist_10 = TPCCUtil.randomStr(24);

                    k++;
                    String str = "";
                    str = str + stock.s_w_id + ",";
                    str = str + stock.s_i_id + ",";
                    str = str + stock.s_quantity + ",";
                    str = str + stock.s_suppkey + ",";
                    str = str + stock.s_ytd + ",";
                    str = str + stock.s_order_cnt + ",";
                    str = str + stock.s_remote_cnt + ",";
                    str = str + stock.s_data + ",";
                    str = str + stock.s_dist_01 + ",";
                    str = str + stock.s_dist_02 + ",";
                    str = str + stock.s_dist_03 + ",";
                    str = str + stock.s_dist_04 + ",";
                    str = str + stock.s_dist_05 + ",";
                    str = str + stock.s_dist_06 + ",";
                    str = str + stock.s_dist_07 + ",";
                    str = str + stock.s_dist_08 + ",";
                    str = str + stock.s_dist_09 + ",";
                    str = str + stock.s_dist_10;
                    out.println(str);

                    if ((k % configCommitCount) == 0) {
                        long tmpTime = new java.util.Date().getTime();
                        String etStr = "  Elasped Time(ms): "
                                + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";
                        LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
                        lastTimeMS = tmpTime;
                    }
                } // end for [w]
            } // end for [i]

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();

            LOG.debug(etStr.substring(0, 30) + "  Writing final records " + k + " of " + t);
            LOG.debug("End Stock Load @  " + now);

            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return k;

    } // end loadStock()

    /**
     * This method combines the semantics of loadCust and loadOrder into a single
     * method, so that the generated timestamps can be split evenly across the ORDER
     * and CUSTOMER tables. Previously, two separate method would increment the clock
     * independently, so that the earliest half of all timestamps ended up in one table
     * and the other half in the other table.
     *
     * The ORDER_LINE table depends on ORDER, so all the ORDER related tables are included
     * as well.
     */
    private int loadTimestamped(int whseKount, int distWhseKount, int custDistKount) {

        int baseRowCount, OrderLineCount, NewOrderCount, total;
        int k = 0;

        Customer customer = new Customer();
        History history = new History();
        Oorder oorder = new Oorder();
        NewOrder new_order = new NewOrder();
        OrderLine order_line = new OrderLine();
        District district = new District();
        Warehouse warehouse = new Warehouse();

        PrintWriter outCust = null;
        PrintWriter outHist = null;
        PrintWriter outOrder = null;
        PrintWriter outNewOrder = null;
        PrintWriter outOrderLine = null;
        PrintWriter outDist = null;
        PrintWriter outWhse = null;

        try {
            LOG.debug("\nWriting Order file to: " + orderPath);
            LOG.debug("\nWriting New Order file to: " + newOrderPath);
            LOG.debug("\nWriting Order Line file to: " + orderLinePath);
            LOG.debug("\nWriting Customer file to: " + customerPath);
            LOG.debug("\nWriting Customer History file to: " + historyPath);

            outOrder = new PrintWriter(new FileOutputStream(orderPath));
            outNewOrder = new PrintWriter(new FileOutputStream(newOrderPath));
            outOrderLine = new PrintWriter(new FileOutputStream(orderLinePath));
            outCust = new PrintWriter(new FileOutputStream(customerPath));
            outHist = new PrintWriter(new FileOutputStream(historyPath));
            outDist = new PrintWriter(new FileOutputStream(districtPath));
            outWhse = new PrintWriter(new FileOutputStream(warehousePath));

            now = new Date();

            baseRowCount = whseKount * distWhseKount * custDistKount;
            OrderLineCount = baseRowCount * 11; // Average of randomly generated value in [5,15], rounded up
            NewOrderCount = baseRowCount / 3; // A third of all NewOrder rows populated from ORDER
            total = baseRowCount * 3 + OrderLineCount + NewOrderCount; // BaseCount * 3: ORDER, CUSTOMER, HISTORY
            total += whseKount + whseKount * distWhseKount; // WAREHOUSE and DISTRICT

            LOG.debug("\nStart timestamped Load for approx. " + total + " timestamped @ " + now + " ...");

            for (int w = 1; w <= whseKount; w++) {

                // ***********************************************
                //          Start of WAREHOUSE population
                // ***********************************************

                warehouse.w_id = w;
                warehouse.w_ytd = 300000;

                // random within [0.0000 .. 0.2000]
                warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0;
                warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, gen));
                warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
                warehouse.w_zip = "123456789";

                // The nationkey is selected via. an alternating region strategy. The entries are selected
                // as the first nation in Africa, the first nation in Asia and so on. When all the regions
                // are covered once, the second nation in each nation is chosen. The nationkey gets the
                // values [1 .. 60], ... , [1 .. 60] as there are 60 nations in total.
                warehouse.w_nationkey = (w - 1) % 60 + 1;

                String str = "";
                str = str + warehouse.w_id + ",";
                str = str + warehouse.w_ytd + ",";
                str = str + warehouse.w_tax + ",";
                str = str + warehouse.w_name + ",";
                str = str + warehouse.w_street_1 + ",";
                str = str + warehouse.w_street_2 + ",";
                str = str + warehouse.w_city + ",";
                str = str + warehouse.w_state + ",";
                str = str + warehouse.w_zip + ",";
                str = str + warehouse.w_nationkey;
                outWhse.println(str);

                for (int d = 1; d <= distWhseKount; d++) {

                    // ***********************************************
                    //             Start of DISTRICT population
                    // ***********************************************

                    district.d_id = d;
                    district.d_w_id = w;
                    district.d_ytd = 30000;

                    // random within [0.0000 .. 0.2000]
                    district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0);

                    district.d_next_o_id = 3001;
                    district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, gen));
                    district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                    district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                    district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                    district.d_state = TPCCUtil.randomStr(3).toUpperCase();
                    district.d_zip = "123456789";
                    district.d_nationkey = RandomParameters.getDistrictNationKey(warehouse.w_nationkey, d);

                    k++;
                    str = "";
                    str = str + district.d_w_id + ",";
                    str = str + district.d_id + ",";
                    str = str + district.d_ytd + ",";
                    str = str + district.d_tax + ",";
                    str = str + district.d_next_o_id + ",";
                    str = str + district.d_name + ",";
                    str = str + district.d_street_1 + ",";
                    str = str + district.d_street_2 + ",";
                    str = str + district.d_city + ",";
                    str = str + district.d_state + ",";
                    str = str + district.d_zip + ",";
                    str = str + district.d_nationkey;
                    outDist.println(str);

                    // ***********************************************
                    //          Prepare CUSTOMER population
                    // ***********************************************

                    // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                    int[] c_ids = new int[custDistKount];
                    for (int i = 0; i < custDistKount; ++i) {
                        c_ids[i] = i + 1;
                    }

                    // Collections.shuffle exists, but there is no Arrays.shuffle
                    for (int i = 0; i < c_ids.length - 1; ++i) {
                        int remaining = c_ids.length - i - 1;
                        int swapIndex = gen.nextInt(remaining) + i + 1;
                        assert i < swapIndex;
                        int temp = c_ids[swapIndex];
                        c_ids[swapIndex] = c_ids[i];
                        c_ids[i] = temp;
                    }

                    for (int c = 1; c <= custDistKount; c++) {

                        // ***********************************************
                        //                Start of CUSTOMER population
                        // ***********************************************

                        // NOTE: The consistency of columns using the sysdate variable as the
                        // datetime is not guaranteed: The columns are never used in either
                        // the transactional or analytical queries. Enforcing consistency on
                        // the variable would logically require that c_since <= o_entry_d,
                        // which would skew the values of o_entry_d, which is often used.
                        // This could of course be handled with some extra work, but that is
                        // deemed unnecessary at this point.
                        Timestamp sysdate = new Timestamp(clock.populateTick());

                        customer.c_id = c;
                        customer.c_d_id = d;
                        customer.c_w_id = w;
                        customer.c_discount = RandomParameters.randDoubleBetween(0, 5000) / 10000; // [0.0000 ... 0.5000]
                        customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, gen));
                        customer.c_credit_lim = 50000;
                        customer.c_balance = -10;
                        customer.c_ytd_payment = 10;
                        customer.c_payment_cnt = 1;
                        customer.c_delivery_cnt = 0;
                        customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                        customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                        customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                        customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
                        customer.c_zip = TPCCUtil.randomNStr(4) + "11111"; // TPC-C 4.3.2.7: 4 random digits + "11111"
                        customer.c_phone = TPCCUtil.randomNStr(16);
                        customer.c_since = sysdate;
                        customer.c_middle = "OE";
                        customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, gen));

                        if (TPCCUtil.randomNumber(1, 100, gen) <= 10) {
                            customer.c_credit = "BC"; // 10% Bad Credit
                        } else {
                            customer.c_credit = "GC"; // 90% Good Credit
                        }
                        if (c <= 1000) {
                            customer.c_last = TPCCUtil.getLastName(c - 1);
                        } else {
                            customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(gen);
                        }

                        str = "";
                        str = str + customer.c_w_id + ",";
                        str = str + customer.c_d_id + ",";
                        str = str + customer.c_id + ",";
                        str = str + customer.c_discount + ",";
                        str = str + customer.c_credit + ",";
                        str = str + customer.c_last + ",";
                        str = str + customer.c_first + ",";
                        str = str + customer.c_credit_lim + ",";
                        str = str + customer.c_balance + ",";
                        str = str + customer.c_ytd_payment + ",";
                        str = str + customer.c_payment_cnt + ",";
                        str = str + customer.c_delivery_cnt + ",";
                        str = str + customer.c_street_1 + ",";
                        str = str + customer.c_street_2 + ",";
                        str = str + customer.c_city + ",";
                        str = str + customer.c_state + ",";
                        str = str + customer.c_zip + ",";
                        str = str + customer.c_phone + ",";
                        str = str + customer.c_since + ",";
                        str = str + customer.c_middle + ",";
                        str = str + customer.c_data;
                        outCust.println(str);
                        k++;

                        // ***********************************************
                        //                Start of HISTORY population
                        // ***********************************************

                        history.h_c_id = c;
                        history.h_c_d_id = d;
                        history.h_c_w_id = w;
                        history.h_d_id = d;
                        history.h_w_id = w;
                        history.h_date = sysdate;
                        history.h_amount = 10;
                        history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, gen));

                        str = "";
                        str = str + history.h_c_id + ",";
                        str = str + history.h_c_d_id + ",";
                        str = str + history.h_c_w_id + ",";
                        str = str + history.h_d_id + ",";
                        str = str + history.h_w_id + ",";
                        str = str + history.h_date + ",";
                        str = str + history.h_amount + ",";
                        str = str + history.h_data;
                        outHist.println(str);
                        k++;

                        // ***********************************************
                        //                Start of ORDER population
                        // ***********************************************

                        oorder.o_entry_d = clock.populateTick();
                        Timestamp entry_d = new java.sql.Timestamp(oorder.o_entry_d);

                        // Count the number of new timestamps (that are relevant for TPCH queries)
                        counter.incrementAndGet();

                        oorder.o_id = c;
                        oorder.o_w_id = w;
                        oorder.o_d_id = d;
                        oorder.o_c_id = c_ids[c - 1];
                        oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);

                        // The variable o_all_local is used in the modified TPCH queries and should
                        // therefore not be a constant value. Set 5 % of all orders as not all local.
                        oorder.o_all_local = RandomParameters.randDoubleBetween(0, 100) <= 5 ? 0 : 1;

                        // o_carrier_id is set *only* for orders with ids < 2101 [4.3.3.1]
                        String o_carrier_id;
                        if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                            oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);
                            o_carrier_id = oorder.o_carrier_id.toString();
                        } else {
                            oorder.o_carrier_id = null;
                            o_carrier_id = nullConstant;
                        }

                        str = "";
                        str = str + oorder.o_w_id + ",";
                        str = str + oorder.o_d_id + ",";
                        str = str + oorder.o_id + ",";
                        str = str + oorder.o_c_id + ",";
                        str = str + o_carrier_id + ",";
                        str = str + oorder.o_ol_cnt + ",";
                        str = str + oorder.o_all_local + ",";
                        str = str + entry_d;
                        outOrder.println(str);
                        k++;

                        // ***********************************************
                        //                Start of NEW_ORDER population
                        // ***********************************************

                        // 900 rows in the NEW-ORDER table corresponding to the last
                        // 900 rows in the ORDER table for that district
                        // (i.e., with NO_O_ID between 2,101 and 3,000)

                        if (c >= FIRST_UNPROCESSED_O_ID) {
                            new_order.no_w_id = w;
                            new_order.no_d_id = d;
                            new_order.no_o_id = c;

                            str = "";
                            str = str + new_order.no_w_id + ",";
                            str = str + new_order.no_d_id + ",";
                            str = str + new_order.no_o_id;
                            outNewOrder.println(str);
                            k++;
                        }

                        // ***********************************************
                        //                Start of ORDER-LINE population
                        // ***********************************************

                        for (int l = 1; l <= oorder.o_ol_cnt; l++) {

                            order_line.ol_w_id = w;
                            order_line.ol_d_id = d;
                            order_line.ol_o_id = c;
                            order_line.ol_number = l; // ol_number
                            order_line.ol_i_id = TPCCUtil.randomNumber(1, 100000, gen);
                            order_line.ol_supply_w_id = order_line.ol_w_id;
                            order_line.ol_quantity = 5;
                            order_line.ol_dist_info = TPCCUtil.randomStr(24);

                            if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                                order_line.ol_delivery_d = oorder.o_entry_d;
                                order_line.ol_amount = 0;
                            } else {
                                // Random between [0.01 .. 9,999.99]
                                order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, gen) / 100.0);
                                order_line.ol_delivery_d = null;
                            }

                            String delivery_d;
                            if (order_line.ol_delivery_d == null) {
                                delivery_d = nullConstant;
                            } else {
                                delivery_d = new Timestamp(order_line.ol_delivery_d).toString();
                            }

                            str = "";
                            str = str + order_line.ol_w_id + ",";
                            str = str + order_line.ol_d_id + ",";
                            str = str + order_line.ol_o_id + ",";
                            str = str + order_line.ol_number + ",";
                            str = str + order_line.ol_i_id + ",";
                            str = str + delivery_d + ",";
                            str = str + order_line.ol_amount + ",";
                            str = str + order_line.ol_supply_w_id + ",";
                            str = str + order_line.ol_quantity + ",";
                            str = str + order_line.ol_dist_info;
                            outOrderLine.println(str);
                            k++;

                            if ((k % configCommitCount) == 0) {
                                long tmpTime = new Date().getTime();
                                String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000)
                                        + "                    ";

                                LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + total);
                                lastTimeMS = tmpTime;
                            }
                        } // end for [l]
                    } // end for [c]
                } // end for [d]
            } // end for [w]

            long tmpTime = new Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + total);
            LOG.debug("End timestamped Data Load @  " + new Date());
            lastTimeMS = tmpTime;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            outCust.close();
            outHist.close();
            outOrder.close();
            outNewOrder.close();
            outOrderLine.close();
            outWhse.close();
            outDist.close();
        }

        return k;
    }

    private int loadRegions() {

        int k = 0;
        int t = 0;
        BufferedReader br;

        try {
            now = new java.util.Date();

            LOG.debug("\nStart Region Load @ " + now + " ...");

            Region region = new Region();
            File file = new File("src", "pt/haslab/htapbench/configuration/tbl/region_gen.tbl");
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();

            out = new PrintWriter(new FileOutputStream(regionPath));
            LOG.debug("\nWriting Order file to: " + regionPath);

            while (line != null) {
                StringTokenizer st = new StringTokenizer(line, "|");
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                region.r_regionkey = Integer.parseInt(st.nextToken());
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                region.r_name = st.nextToken();
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                region.r_comment = st.nextToken();
                if (st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }

                k++;

                String str = "";
                str = str + region.r_regionkey + ",";
                str = str + region.r_name + ",";
                str = str + region.r_comment;
                out.println(str);

                line = br.readLine();
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();
            out.close();

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
            LOG.debug("End Supplier Data Load @  " + now);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return k;

    } // end loadRegions()

    private int loadNations() {

        int k = 0;
        int t = 0;
        BufferedReader br;

        try {
            now = new java.util.Date();
            LOG.debug("\nStart Nation Load @ " + now + " ...");

            Nation nation = new Nation();
            File file = new File("src", "pt/haslab/htapbench/configuration/tbl/nation_gen.tbl");
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();

            out = new PrintWriter(new FileOutputStream(nationPath));
            LOG.debug("\nWriting Order file to: " + nationPath);

            while (line != null) {
                StringTokenizer st = new StringTokenizer(line, "|");
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                nation.n_nationkey = Integer.parseInt(st.nextToken());
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                nation.n_name = st.nextToken();
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                nation.n_regionkey = Integer.parseInt(st.nextToken());
                if (!st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }
                nation.n_comment = st.nextToken();
                if (st.hasMoreTokens()) {
                    LOG.error("invalid input file: " + file.getAbsolutePath());
                }

                k++;

                String str = "";
                str = str + nation.n_nationkey + ",";
                str = str + nation.n_name + ",";
                str = str + nation.n_regionkey + ",";
                str = str + nation.n_comment;
                out.println(str);
                line = br.readLine();
            }
            out.close();

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
            LOG.debug("End Region Load @  " + now);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return k;
    } // end loadNations()

    private int loadSuppliers() {

        int k = 0;
        int t = 0;

        try {
            out = new PrintWriter(new FileOutputStream(supplierPath));
            LOG.debug("\nWriting supplier file to: " + supplierPath);

            now = new java.util.Date();
            LOG.debug("\nStart Supplier Load @ " + now + " ...");
            Supplier supplier = new Supplier();

            for (int index = 1; index <= 10000; index++) {
                supplier.su_suppkey = index;
                supplier.su_name = ran.astring(25, 25);
                supplier.su_address = ran.astring(20, 40);
                supplier.su_nationkey = randomParam.getRandomNationKey();
                supplier.su_phone = ran.nstring(15, 15);
                supplier.su_acctbal = ran.fixedPoint(2, 10000., 10000000.);
                supplier.su_comment = ran.astring(51, 101);
                k++;

                String str = "";
                str = str + supplier.su_suppkey + ",";
                str = str + supplier.su_name + ",";
                str = str + supplier.su_address + ",";
                str = str + supplier.su_nationkey + ",";
                str = str + supplier.su_phone + ",";
                str = str + supplier.su_acctbal + ",";
                str = str + supplier.su_comment;
                out.println(str);

            }
            out.close();

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
            LOG.debug("End Supplier Data Load @  " + now);
        } catch (Exception e) {
            e.printStackTrace();
            out.close();
        }

        return k;
    } // end loadSuppliers()

    @Override
    public void load() {
        gen = new Random(clock.getStartTimestamp());

        // The number of timestamps generated in the population phase is known.
        // This information is used to compute the start- and end timestamp of
        // the generated timestamps, which is then written to an output file,
        // so the Clock in the execution phase can be initialized with those
        // values.
        long startTS = clock.getStartTimestamp();
        long finalTS = clock.getFinalPopulatedTs();
        AuxiliarFileHandler.writeToFile(csvFilePath, startTS, finalTS);

        Date startDate = new Date();
        LOG.info("Generating CSV files ... This may take a while");
        LOG.debug("------------- LoadData Start Date = " + startDate + "-------------");

        long startTimeMS = new Date().getTime();
        lastTimeMS = startTimeMS;

        long totalRows = loadItem(configItemCount);
        totalRows += loadTimestamped(numWarehouses, configDistPerWhse, configCustPerDist);
        totalRows += loadStock(numWarehouses, configItemCount);
        totalRows += loadRegions();
        totalRows += loadNations();
        totalRows += loadSuppliers();

        long runTimeMS = (new Date().getTime()) + 1 - startTimeMS;
        Date endDate = new Date();

        LOG.info("");
        LOG.info("------------- LoadJDBC Statistics --------------------");
        LOG.info("     Start Time = " + startDate);
        LOG.info("       End Time = " + endDate);
        LOG.info("       Run Time = " + (int) runTimeMS / 1000 + " Seconds");
        LOG.info("    Rows Loaded = " + totalRows + " Rows");
        LOG.info("Rows Per Second = " + (totalRows / (runTimeMS / 1000)) + " Rows/Sec");
        LOG.info(" Clock Start TS = " + new Timestamp(startTS));
        LOG.info("   Clock End TS = " + new Timestamp(finalTS));
        LOG.info("Total # of New Timestamps (Load Stage): " + counter.get());
        LOG.info("------------------------------------------------------");
    }

} // end LoadData Class
