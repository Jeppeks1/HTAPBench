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

import pt.haslab.htapbench.core.AuxiliarFileHandler;

import static pt.haslab.htapbench.benchmark.jTPCCConfig.configCommitCount;
import static pt.haslab.htapbench.benchmark.jTPCCConfig.configCustPerDist;
import static pt.haslab.htapbench.benchmark.jTPCCConfig.configDistPerWhse;
import static pt.haslab.htapbench.benchmark.jTPCCConfig.configItemCount;
import static pt.haslab.htapbench.benchmark.jTPCCConfig.configWhseCount;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.Loader;
import pt.haslab.htapbench.pojo.Nation;
import pt.haslab.htapbench.pojo.Region;
import pt.haslab.htapbench.pojo.Supplier;
import pt.haslab.htapbench.pojo.Customer;
import pt.haslab.htapbench.pojo.District;
import pt.haslab.htapbench.pojo.History;
import pt.haslab.htapbench.pojo.Item;
import pt.haslab.htapbench.pojo.NewOrder;
import pt.haslab.htapbench.pojo.Oorder;
import pt.haslab.htapbench.pojo.OrderLine;
import pt.haslab.htapbench.pojo.Stock;
import pt.haslab.htapbench.pojo.Warehouse;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.densitity.DensityConsultant;
import pt.haslab.htapbench.util.RandomGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTAPB database Loader.
 *
 * @author Fábio Coelho <fabio.a.coelho@inesctec.pt>
 */

public class HTAPBCSVLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(HTAPBCSVLoader.class);
    private Clock clock;
    private String fileLocation;

    HTAPBCSVLoader(HTAPBenchmark benchmark, boolean calibrate, String outputPath, DensityConsultant density) {
        super(benchmark, null);

        numWarehouses = (int) Math.round(configWhseCount * this.scaleFactor);

        if (numWarehouses == 0)
            numWarehouses = 1;

        this.fileLocation = outputPath + '/';
        this.calibrate = calibrate;
        this.clock = new Clock(density.getDeltaTs(), (int) benchmark.getWorkloadConfiguration().getScaleFactor(), true);
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

    private boolean calibrate;
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
            out = new PrintWriter(new FileOutputStream(fileLocation + "item.csv"));
            t = itemKount;

            LOG.debug("\nStart Item Load for " + t + " Items @ " + now + " ...");
            LOG.debug("\nWriting Item file to: " + fileLocation + "item.csv");

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
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1)
                            + "ORIGINAL"
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

    private int loadWhse(int whseKount) {

        try {
            now = new java.util.Date();
            Warehouse warehouse = new Warehouse();

            out = new PrintWriter(new FileOutputStream(fileLocation + "warehouse.csv"));

            LOG.debug("\nStart Whse Load for " + whseKount + " Whses @ " + now + " ...");
            LOG.debug("\nWriting Warehouse file to: " + fileLocation + "warehouse.csv");

            for (int i = 1; i <= whseKount; i++) {

                warehouse.w_id = i;
                warehouse.w_ytd = 300000;

                // random within [0.0000 .. 0.2000]
                warehouse.w_tax = (TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0;
                warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, gen));
                warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, gen));
                warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
                warehouse.w_zip = "123456789";

                String str = "";
                str = str + warehouse.w_id + ",";
                str = str + warehouse.w_ytd + ",";
                str = str + warehouse.w_tax + ",";
                str = str + warehouse.w_name + ",";
                str = str + warehouse.w_street_1 + ",";
                str = str + warehouse.w_street_2 + ",";
                str = str + warehouse.w_city + ",";
                str = str + warehouse.w_state + ",";
                str = str + warehouse.w_zip;
                out.println(str);
            } // end for

            now = new java.util.Date();

            long tmpTime = new java.util.Date().getTime();
            lastTimeMS = tmpTime;

            LOG.debug("Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000));
            LOG.debug("End Whse Load @  " + now);

            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return whseKount;

    } // end loadWhse()

    private int loadStock(int whseKount, int itemKount) {

        int k = 0;
        int t;
        int randPct;
        int len;
        int startORIGINAL;

        try {
            now = new java.util.Date();
            t = (whseKount * itemKount);

            out = new PrintWriter(new FileOutputStream(fileLocation + "stock.csv"));

            LOG.debug("\nStart Stock Load for " + t + " units @ " + now + " ...");
            LOG.debug("\nWriting Stock file to: " + fileLocation + "stock.csv");

            Stock stock = new Stock();

            for (int i = 1; i <= itemKount; i++) {
                for (int w = 1; w <= whseKount; w++) {
                    stock.s_i_id = i;
                    stock.s_w_id = w;
                    stock.s_quantity = TPCCUtil.randomNumber(10, 100, gen);
                    stock.s_ytd = 0;
                    stock.s_order_cnt = 0;
                    stock.s_remote_cnt = 0;

                    // s_data
                    randPct = TPCCUtil.randomNumber(1, 100, gen);
                    len = TPCCUtil.randomNumber(26, 50, gen);
                    if (randPct > 10) {
                        // 90% of time i_data isa random string of length [26 ..
                        // 50]
                        stock.s_data = TPCCUtil.randomStr(len);
                    } else {
                        // 10% of time i_data has "ORIGINAL" crammed somewhere
                        // in middle
                        startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
                        stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1)
                                + "ORIGINAL"
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

    private int loadDist(int whseKount, int distWhseKount) {

        int k = 0;
        int t;

        try {
            now = new java.util.Date();

            out = new PrintWriter(new FileOutputStream(fileLocation + "district.csv"));
            LOG.debug("\nWriting District file to: " + fileLocation + "district.csv");

            District district = new District();

            t = (whseKount * distWhseKount);
            LOG.debug("\nStart District Data for " + t + " Dists @ " + now + " ...");

            for (int w = 1; w <= whseKount; w++) {
                for (int d = 1; d <= distWhseKount; d++) {
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

                    k++;
                    String str = "";
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
                    str = str + district.d_zip;
                    out.println(str);
                } // end for [d]
            } // end for [w]

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
            LOG.debug("End District Load @  " + now);

            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return k;

    } // end loadDist()

    private int loadCust(int whseKount, int distWhseKount, int custDistKount) {

        int k = 0;
        int t;

        Customer customer = new Customer();
        History history = new History();
        PrintWriter outHist = null;
        PrintWriter outcost = null;

        try {
            now = new java.util.Date();

            outcost = new PrintWriter(new FileOutputStream(fileLocation + "customer.csv"));
            outHist = new PrintWriter(new FileOutputStream(fileLocation + "cust-hist.csv"));

            LOG.debug("\nWriting Customer file to: " + fileLocation + "customer.csv");
            LOG.debug("\nWriting Customer History file to: " + fileLocation + "cust-hist.csv");

            t = (whseKount * distWhseKount * custDistKount * 2);
            LOG.debug("\nStart Cust-Hist Load for " + t + " Cust-Hists @ " + now + " ...");

            for (int w = 1; w <= whseKount; w++) {

                for (int d = 1; d <= distWhseKount; d++) {

                    for (int c = 1; c <= custDistKount; c++) {

                        //**************************************************
                        //TIMESTAMP DENSITY
                        Timestamp sysdate = new java.sql.Timestamp(clock.tick());
                        //**************************************************

                        if (calibrate) {
                            counter.incrementAndGet();
                        }

                        customer.c_id = c;
                        customer.c_d_id = d;
                        customer.c_w_id = w;

                        // discount is random between [0.0000 ... 0.5000]
                        customer.c_discount = (TPCCUtil.randomNumber(1, 5000, gen) / 1000.0);

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
                        // TPC-C 4.3.2.7: 4 random digits + "11111"
                        customer.c_zip = TPCCUtil.randomNStr(4) + "11111";

                        customer.c_phone = TPCCUtil.randomNStr(16);

                        customer.c_since = sysdate;
                        customer.c_middle = "OE";
                        customer.c_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, gen));

                        history.h_c_id = c;
                        history.h_c_d_id = d;
                        history.h_c_w_id = w;
                        history.h_d_id = d;
                        history.h_w_id = w;
                        history.h_date = sysdate;
                        history.h_amount = 10;
                        history.h_data = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, gen));

                        k = k + 2;
                        String str = "";
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
                        outcost.println(str);

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

                        if ((k % configCommitCount) == 0) {
                            long tmpTime = new java.util.Date().getTime();
                            String etStr = "  Elasped Time(ms): "
                                    + ((tmpTime - lastTimeMS) / 1000.000)
                                    + "                    ";
                            LOG.debug(etStr.substring(0, 30)
                                    + "  Writing record " + k + " of " + t);
                            lastTimeMS = tmpTime;
                        }

                    } // end for [c]
                } // end for [d]
            } // end for [w]

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";

            lastTimeMS = tmpTime;
            now = new java.util.Date();

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
            LOG.debug("End Cust-Hist Data Load @  " + now);

            outHist.close();
            outcost.close();
        } catch (Exception e) {
            e.printStackTrace();
            outHist.close();
            outcost.close();
        }

        return k;

    } // end loadCust()

    private int loadOrder(int whseKount, int distWhseKount, int custDistKount) {

        int k = 0;
        int t;
        PrintWriter outLine = null;
        PrintWriter outNewOrder = null;

        try {
            out = new PrintWriter(new FileOutputStream(fileLocation + "order.csv"));
            outLine = new PrintWriter(new FileOutputStream(fileLocation + "order-line.csv"));
            outNewOrder = new PrintWriter(new FileOutputStream(fileLocation + "new-order.csv"));

            LOG.debug("\nWriting Order file to: " + fileLocation + "order.csv");
            LOG.debug("\nWriting Order Line file to: " + fileLocation + "order-line.csv");
            LOG.debug("\nWriting New Order file to: " + fileLocation + "new-order.csv");

            now = new java.util.Date();
            Oorder oorder = new Oorder();
            NewOrder new_order = new NewOrder();
            OrderLine order_line = new OrderLine();

            t = (whseKount * distWhseKount * custDistKount);
            t = (t * 11) + (t / 3);

            LOG.debug("whse=" + whseKount + ", dist=" + distWhseKount + ", cust=" + custDistKount);
            LOG.debug("\nStart Order-Line-New Load for approx " + t + " rows @ " + now + " ...");

            for (int w = 1; w <= whseKount; w++) {
                for (int d = 1; d <= distWhseKount; d++) {
                    // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                    int[] c_ids = new int[custDistKount];
                    for (int i = 0; i < custDistKount; ++i) {
                        c_ids[i] = i + 1;
                    }
                    // Collections.shuffle exists, but there is no
                    // Arrays.shuffle
                    for (int i = 0; i < c_ids.length - 1; ++i) {
                        int remaining = c_ids.length - i - 1;
                        int swapIndex = gen.nextInt(remaining) + i + 1;
                        assert i < swapIndex;
                        int temp = c_ids[swapIndex];
                        c_ids[swapIndex] = c_ids[i];
                        c_ids[i] = temp;
                    }

                    for (int c = 1; c <= custDistKount; c++) {

                        oorder.o_id = c;
                        oorder.o_w_id = w;
                        oorder.o_d_id = d;
                        oorder.o_c_id = c_ids[c - 1];
                        // o_carrier_id is set *only* for orders with ids < 2101
                        // [4.3.3.1]
                        if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                            oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10,
                                    gen);
                        } else {
                            oorder.o_carrier_id = null;
                        }
                        oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);
                        oorder.o_all_local = 1;

                        //**************************************************
                        //TIMESTAMP DENSITY
                        oorder.o_entry_d = clock.tick();
                        //**************************************************

                        if (calibrate) {
                            counter.incrementAndGet();
                        }

                        k++;
                        String str = "";
                        str = str + oorder.o_id + ",";
                        str = str + oorder.o_w_id + ",";
                        str = str + oorder.o_d_id + ",";
                        str = str + oorder.o_c_id + ",";
                        str = str + oorder.o_carrier_id + ",";
                        str = str + oorder.o_ol_cnt + ",";
                        str = str + oorder.o_all_local + ",";
                        Timestamp entry_d = new java.sql.Timestamp(oorder.o_entry_d);
                        str = str + entry_d;
                        out.println(str);

                        // 900 rows in the NEW-ORDER table corresponding to the
                        // last
                        // 900 rows in the ORDER table for that district (i.e.,
                        // with
                        // NO_O_ID between 2,101 and 3,000)

                        if (c >= FIRST_UNPROCESSED_O_ID) {

                            new_order.no_w_id = w;
                            new_order.no_d_id = d;
                            new_order.no_o_id = c;

                            k++;
                            str = "";
                            str = str + new_order.no_w_id + ",";
                            str = str + new_order.no_d_id + ",";
                            str = str + new_order.no_o_id;
                            outNewOrder.println(str);


                        } // end new order

                        for (int l = 1; l <= oorder.o_ol_cnt; l++) {
                            order_line.ol_w_id = w;
                            order_line.ol_d_id = d;
                            order_line.ol_o_id = c;
                            order_line.ol_number = l; // ol_number
                            order_line.ol_i_id = TPCCUtil.randomNumber(1, 100000, gen);
                            if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                                order_line.ol_delivery_d = oorder.o_entry_d;
                                order_line.ol_amount = 0;
                            } else {
                                order_line.ol_delivery_d = null;
                                // random within [0.01 .. 9,999.99]
                                order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, gen) / 100.0);
                            }

                            order_line.ol_supply_w_id = order_line.ol_w_id;
                            order_line.ol_quantity = 5;
                            order_line.ol_dist_info = TPCCUtil.randomStr(24);

                            k++;
                            str = "";
                            str = str + order_line.ol_w_id + ",";
                            str = str + order_line.ol_d_id + ",";
                            str = str + order_line.ol_o_id + ",";
                            str = str + order_line.ol_number + ",";
                            str = str + order_line.ol_i_id + ",";
                            Timestamp delivery_d;
                            if (order_line.ol_delivery_d == null) {
                                delivery_d = new Timestamp(0L);
                            } else {
                                delivery_d = new Timestamp(order_line.ol_delivery_d);
                            }
                            str = str + delivery_d + ",";
                            str = str + order_line.ol_amount + ",";
                            str = str + order_line.ol_supply_w_id + ",";
                            str = str + order_line.ol_quantity + ",";
                            str = str + order_line.ol_dist_info;
                            outLine.println(str);

                            if ((k % configCommitCount) == 0) {
                                long tmpTime = new java.util.Date().getTime();
                                String etStr = "  Elasped Time(ms): "
                                        + ((tmpTime - lastTimeMS) / 1000.000) + "                    ";
                                LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
                                lastTimeMS = tmpTime;
                                out.close();
                            }
                        } // end for [l]
                    } // end for [c]
                } // end for [d]
            } // end for [w]

            LOG.debug("  Writing final records " + k + " of " + t);
            outLine.close();
            outNewOrder.close();
            out.close();
            now = new java.util.Date();
            LOG.debug("End Orders Load @  " + now);

        } catch (Exception e) {
            e.printStackTrace();
            outLine.close();
            outNewOrder.close();
        }
        return k;

    } // end loadOrder()

    private int loadRegions() {

        int k = 0;
        int t = 0;
        BufferedReader br;

        try {
            now = new java.util.Date();

            LOG.debug("\nStart Region Load @ " + now + " ...");

            Region region = new Region();
            File file = new File("src", "pt/haslab/htapbench/benchmark/region_gen.tbl");
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();

            out = new PrintWriter(new FileOutputStream(fileLocation + "region.csv"));
            LOG.debug("\nWriting Order file to: " + fileLocation + "region.csv");

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
            File file = new File("src", "pt/haslab/htapbench/benchmark/nation_gen.tbl");
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();

            out = new PrintWriter(new FileOutputStream(fileLocation + "nation.csv"));
            LOG.debug("\nWriting Order file to: " + fileLocation + "nation.csv");

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
            out = new PrintWriter(new FileOutputStream(fileLocation + "supplier.csv"));
            LOG.debug("\nWriting supplier file to: " + fileLocation + "supplier.csv");

            now = new java.util.Date();
            LOG.debug("\nStart Supplier Load @ " + now + " ...");
            Supplier supplier = new Supplier();

            for (int index = 1; index <= 10000; index++) {
                supplier.su_suppkey = index;
                supplier.su_name = ran.astring(25, 25);
                supplier.su_address = ran.astring(20, 40);
                supplier.su_nationkey = nationkeys[ran.number(0, 61)];
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
        AuxiliarFileHandler.writeToFile("./", startTS, finalTS);

        Date startDate = new Date();
        LOG.debug("------------- LoadData Start Date = " + startDate + "-------------");

        long startTimeMS = new Date().getTime();
        lastTimeMS = startTimeMS;

        long totalRows = loadWhse(numWarehouses);
        totalRows += loadItem(configItemCount);
        totalRows += loadStock(numWarehouses, configItemCount);
        totalRows += loadDist(numWarehouses, configDistPerWhse);
        totalRows += loadCust(numWarehouses, configDistPerWhse, configCustPerDist);
        totalRows += loadOrder(numWarehouses, configDistPerWhse, configCustPerDist);
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
