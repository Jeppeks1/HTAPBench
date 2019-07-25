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

import pt.haslab.htapbench.benchmark.AuxiliarFileHandler;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.core.HTAPBenchmark;
import pt.haslab.htapbench.util.TPCCUtil;

import static pt.haslab.htapbench.benchmark.HTAPBConstants.configCommitCount;
import static pt.haslab.htapbench.benchmark.HTAPBConstants.configCustPerDist;
import static pt.haslab.htapbench.benchmark.HTAPBConstants.configDistPerWhse;
import static pt.haslab.htapbench.benchmark.HTAPBConstants.configItemCount;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.configuration.loader.pojo.Nation;
import pt.haslab.htapbench.configuration.loader.pojo.Region;
import pt.haslab.htapbench.configuration.loader.pojo.Supplier;
import pt.haslab.htapbench.jdbc.jdbcIO;
import pt.haslab.htapbench.configuration.loader.pojo.Customer;
import pt.haslab.htapbench.configuration.loader.pojo.District;
import pt.haslab.htapbench.configuration.loader.pojo.History;
import pt.haslab.htapbench.configuration.loader.pojo.Item;
import pt.haslab.htapbench.configuration.loader.pojo.NewOrder;
import pt.haslab.htapbench.configuration.loader.pojo.Oorder;
import pt.haslab.htapbench.configuration.loader.pojo.OrderLine;
import pt.haslab.htapbench.configuration.loader.pojo.Stock;
import pt.haslab.htapbench.configuration.loader.pojo.Warehouse;
import pt.haslab.htapbench.core.Clock;
import pt.haslab.htapbench.core.DensityConsultant;
import pt.haslab.htapbench.random.RandomGenerator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * HTAPB database Loader.
 *
 * @author Fábio Coelho <fabio.a.coelho@inesctec.pt>
 */

public class HTAPBLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(HTAPBLoader.class);

    public HTAPBLoader(HTAPBenchmark benchmark, Connection c) {
        super(benchmark, c);

        numWarehouses = (int) Math.round(this.scaleFactor);
        if (numWarehouses == 0) {
            numWarehouses = 1;
        }

        counter = new AtomicInteger();
    }

    // ********** general vars **********************************
    private static java.util.Date now = null;

    private static PrintWriter out = null;
    private static Random gen;
    private static int numWarehouses = 0;
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

    private void transRollback() {
        if (!generateCsvFiles) {
            try {
                conn.rollback();
            } catch (SQLException se) {
                LOG.debug(se.getMessage());
            }
        } else {
            out.close();
        }
    }

    private void transCommit() {
        if (!generateCsvFiles) {
            try {
                conn.commit();
            } catch (SQLException se) {
                LOG.debug(se.getMessage());
                transRollback();
            }
        } else {
            out.close();
        }
    }

    private void truncateTable(String strTable) {
        LOG.debug("Truncating '" + strTable + "' ...");
        try {
            this.conn.createStatement().execute("DELETE FROM " + strTable);
            transCommit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback();
        }
    }

    private int loadItem(int itemKount) {

        int k = 0;
        int t;
        int randPct;
        int len;
        int startORIGINAL;

        try {
            PreparedStatement itemPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_ITEM + " VALUES (?,?,?,?,?)");

            now = new java.util.Date();
            t = itemKount;
            LOG.debug("\nStart Item Load for " + t + " Items @ " + now + " ...");

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(itemPath));
                LOG.debug("\nWriting Item file to: " + itemPath);
            }

            Item item = new Item();

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

                if (!generateCsvFiles) {
                    itemPrepStmt.setLong(1, item.i_id);
                    itemPrepStmt.setString(2, item.i_name);
                    itemPrepStmt.setDouble(3, item.i_price);
                    itemPrepStmt.setString(4, item.i_data);
                    itemPrepStmt.setLong(5, item.i_im_id);
                    itemPrepStmt.addBatch();

                    if ((k % configCommitCount) == 0) {
                        long tmpTime = new java.util.Date().getTime();
                        String etStr = "  Elasped Time(ms): "
                                + ((tmpTime - lastTimeMS) / 1000.000)
                                + "                    ";
                        LOG.debug(etStr.substring(0, 30)
                                + "  Writing record " + k + " of " + t);

                        lastTimeMS = tmpTime;
                        itemPrepStmt.executeBatch();
                        itemPrepStmt.clearBatch();

                        transCommit();
                    }
                } else {
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
                                + ((tmpTime - lastTimeMS) / 1000.000)
                                + "                    ";
                        LOG.debug(etStr.substring(0, 30)
                                + "  Writing record " + k + " of " + t);

                        lastTimeMS = tmpTime;
                    }
                }

            } // end for

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);

            lastTimeMS = tmpTime;

            if (!generateCsvFiles) {
                itemPrepStmt.executeBatch();
            }

            transCommit();
            now = new java.util.Date();
            LOG.debug("End Item Load @  " + now);
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            se.printStackTrace();
            SQLException next = se.getNextException();

            if (next != null)
                LOG.debug(next.getMessage());

            transRollback();
        } catch (Exception e) {
            e.printStackTrace();
            transRollback();
        }

        return k;

    } // end loadItem()

    private int loadWhse(int whseKount) {

        try {
            PreparedStatement whsePrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_WAREHOUSE + " VALUES(?,?,?,?,?,?,?,?,?)");

            now = new java.util.Date();
            LOG.debug("\nStart Whse Load for " + whseKount + " Whses @ " + now + " ...");

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(warehousePath));
                LOG.debug("\nWriting Warehouse file to: " + warehousePath);
            }

            Warehouse warehouse = new Warehouse();
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

                if (!generateCsvFiles) {
                    whsePrepStmt.setLong(1, warehouse.w_id);
                    whsePrepStmt.setDouble(2, warehouse.w_ytd);
                    whsePrepStmt.setDouble(3, warehouse.w_tax);
                    whsePrepStmt.setString(4, warehouse.w_name);
                    whsePrepStmt.setString(5, warehouse.w_street_1);
                    whsePrepStmt.setString(6, warehouse.w_street_2);
                    whsePrepStmt.setString(7, warehouse.w_city);
                    whsePrepStmt.setString(8, warehouse.w_state);
                    whsePrepStmt.setString(9, warehouse.w_zip);
                    whsePrepStmt.executeUpdate();
                } else {
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
                }
            } // end for

            transCommit();
            now = new java.util.Date();

            long tmpTime = new java.util.Date().getTime();
            LOG.debug("Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000));
            lastTimeMS = tmpTime;
            LOG.debug("End Whse Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback();
        } catch (Exception e) {
            e.printStackTrace();
            transRollback();
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
            PreparedStatement stckPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_STOCK + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            now = new java.util.Date();
            t = (whseKount * itemKount);
            LOG.debug("\nStart Stock Load for " + t + " units @ " + now + " ...");

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(stockPath));
                LOG.debug("\nWriting Stock file to: " + stockPath);
            }

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
                    if (!generateCsvFiles) {
                        stckPrepStmt.setLong(1, stock.s_w_id);
                        stckPrepStmt.setLong(2, stock.s_i_id);
                        stckPrepStmt.setLong(3, stock.s_quantity);
                        stckPrepStmt.setDouble(4, stock.s_ytd);
                        stckPrepStmt.setLong(5, stock.s_order_cnt);
                        stckPrepStmt.setLong(6, stock.s_remote_cnt);
                        stckPrepStmt.setString(7, stock.s_data);
                        stckPrepStmt.setString(8, stock.s_dist_01);
                        stckPrepStmt.setString(9, stock.s_dist_02);
                        stckPrepStmt.setString(10, stock.s_dist_03);
                        stckPrepStmt.setString(11, stock.s_dist_04);
                        stckPrepStmt.setString(12, stock.s_dist_05);
                        stckPrepStmt.setString(13, stock.s_dist_06);
                        stckPrepStmt.setString(14, stock.s_dist_07);
                        stckPrepStmt.setString(15, stock.s_dist_08);
                        stckPrepStmt.setString(16, stock.s_dist_09);
                        stckPrepStmt.setString(17, stock.s_dist_10);
                        stckPrepStmt.addBatch();

                        if ((k % configCommitCount) == 0) {
                            long tmpTime = new java.util.Date().getTime();
                            String etStr = "  Elasped Time(ms): "
                                    + ((tmpTime - lastTimeMS) / 1000.000)
                                    + "                    ";
                            LOG.debug(etStr.substring(0, 30)
                                    + "  Writing record " + k + " of " + t);
                            lastTimeMS = tmpTime;

                            stckPrepStmt.executeBatch();
                            stckPrepStmt.clearBatch();

                            transCommit();
                        }
                    } else {
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
                                    + ((tmpTime - lastTimeMS) / 1000.000)
                                    + "                    ";
                            LOG.debug(etStr.substring(0, 30)
                                    + "  Writing record " + k + " of " + t);
                            lastTimeMS = tmpTime;
                        }
                    }

                } // end for [w]

            } // end for [i]

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug(etStr.substring(0, 30)
                    + "  Writing final records " + k + " of " + t);
            lastTimeMS = tmpTime;
            if (!generateCsvFiles) {
                stckPrepStmt.executeBatch();
            }
            transCommit();

            now = new java.util.Date();
            LOG.debug("End Stock Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback();

        } catch (Exception e) {
            e.printStackTrace();
            transRollback();
        }

        return k;

    } // end loadStock()

    private int loadDist(int whseKount, int distWhseKount) {

        int k = 0;
        int t;

        try {
            PreparedStatement distPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_DISTRICT + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            now = new java.util.Date();

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(districtPath));
                LOG.debug("\nWriting District file to: " + districtPath);
            }

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
                    if (!generateCsvFiles) {
                        distPrepStmt.setLong(1, district.d_w_id);
                        distPrepStmt.setLong(2, district.d_id);
                        distPrepStmt.setDouble(3, district.d_ytd);
                        distPrepStmt.setDouble(4, district.d_tax);
                        distPrepStmt.setLong(5, district.d_next_o_id);
                        distPrepStmt.setString(6, district.d_name);
                        distPrepStmt.setString(7, district.d_street_1);
                        distPrepStmt.setString(8, district.d_street_2);
                        distPrepStmt.setString(9, district.d_city);
                        distPrepStmt.setString(10, district.d_state);
                        distPrepStmt.setString(11, district.d_zip);
                        distPrepStmt.executeUpdate();
                    } else {
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
                    }
                } // end for [d]
            } // end for [w]

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";

            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);

            lastTimeMS = tmpTime;
            transCommit();
            now = new java.util.Date();
            LOG.debug("End District Load @  " + now);
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback();
        } catch (Exception e) {
            e.printStackTrace();
            transRollback();
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
            PreparedStatement histPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_HISTORY + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            PreparedStatement custPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_CUSTOMER + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            now = new java.util.Date();

            if (generateCsvFiles) {
                outcost = new PrintWriter(new FileOutputStream(customerPath));
                outHist = new PrintWriter(new FileOutputStream(historyPath));
                LOG.debug("\nWriting Customer file to: " + customerPath);
                LOG.debug("\nWriting Customer History file to: " + historyPath);
            }

            t = (whseKount * distWhseKount * custDistKount * 2);
            LOG.debug("\nStart Cust-Hist Load for " + t + " Cust-Hists @ " + now + " ...");

            for (int w = 1; w <= whseKount; w++) {

                for (int d = 1; d <= distWhseKount; d++) {

                    for (int c = 1; c <= custDistKount; c++) {

                        //**************************************************
                        //TIMESTAMP DENSITY
                        Timestamp sysdate = new java.sql.Timestamp(clock.populateTick());
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
                        customer.c_zip = TPCCUtil.randomNStr(4) + "11111"; // TPC-C 4.3.2.7: 4 random digits + "11111"
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
                        history.h_data = TPCCUtil.randomStr(TPCCUtil
                                .randomNumber(10, 24, gen));

                        k = k + 2;
                        if (!generateCsvFiles) {
                            custPrepStmt.setLong(1, customer.c_w_id);
                            custPrepStmt.setLong(2, customer.c_d_id);
                            custPrepStmt.setLong(3, customer.c_id);
                            custPrepStmt.setDouble(4, customer.c_discount);
                            custPrepStmt.setString(5, customer.c_credit);
                            custPrepStmt.setString(6, customer.c_last);
                            custPrepStmt.setString(7, customer.c_first);
                            custPrepStmt.setDouble(8, customer.c_credit_lim);
                            custPrepStmt.setDouble(9, customer.c_balance);
                            custPrepStmt.setDouble(10, customer.c_ytd_payment);
                            custPrepStmt.setLong(11, customer.c_payment_cnt);
                            custPrepStmt.setLong(12, customer.c_delivery_cnt);
                            custPrepStmt.setString(13, customer.c_street_1);
                            custPrepStmt.setString(14, customer.c_street_2);
                            custPrepStmt.setString(15, customer.c_city);
                            custPrepStmt.setString(16, customer.c_state);
                            custPrepStmt.setString(17, customer.c_zip);
                            custPrepStmt.setString(18, customer.c_phone);

                            custPrepStmt.setTimestamp(19, customer.c_since);
                            custPrepStmt.setString(20, customer.c_middle);
                            custPrepStmt.setString(21, customer.c_data);

                            custPrepStmt.addBatch();

                            histPrepStmt.setInt(1, history.h_c_id);
                            histPrepStmt.setInt(2, history.h_c_d_id);
                            histPrepStmt.setInt(3, history.h_c_w_id);

                            histPrepStmt.setInt(4, history.h_d_id);
                            histPrepStmt.setInt(5, history.h_w_id);
                            histPrepStmt.setTimestamp(6, history.h_date);
                            histPrepStmt.setDouble(7, history.h_amount);
                            histPrepStmt.setString(8, history.h_data);

                            histPrepStmt.addBatch();

                            if ((k % configCommitCount) == 0) {
                                long tmpTime = new java.util.Date().getTime();
                                String etStr = "  Elasped Time(ms): "
                                        + ((tmpTime - lastTimeMS) / 1000.000)
                                        + "                    ";
                                LOG.debug(etStr.substring(0, 30)
                                        + "  Writing record " + k + " of " + t);
                                lastTimeMS = tmpTime;

                                custPrepStmt.executeBatch();
                                histPrepStmt.executeBatch();
                                custPrepStmt.clearBatch();
                                custPrepStmt.clearBatch();

                                transCommit();
                            }
                        } else {
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
                        }
                    } // end for [c]
                } // end for [d]
            } // end for [w]

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
                    + " of " + t);
            lastTimeMS = tmpTime;

            custPrepStmt.executeBatch();
            histPrepStmt.executeBatch();
            custPrepStmt.clearBatch();
            histPrepStmt.clearBatch();

            transCommit();
            now = new java.util.Date();
            if (generateCsvFiles) {
                outHist.close();
                outcost.close();
            }
            LOG.debug("End Cust-Hist Data Load @  " + now);
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback();
            if (generateCsvFiles) {
                outHist.close();
                outcost.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            transRollback();
            if (generateCsvFiles) {
                outHist.close();
                outcost.close();
            }
        }

        return k;

    } // end loadCust()

    private int loadOrder(int whseKount, int distWhseKount, int custDistKount) {

        int k = 0;
        int t;
        PrintWriter outLine = null;
        PrintWriter outNewOrder = null;

        try {
            PreparedStatement ordrPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_ORDER + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            PreparedStatement nworPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_NEWORDER + " VALUES (?, ?, ?)");
            PreparedStatement orlnPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_ORDERLINE + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(orderPath));
                outLine = new PrintWriter(new FileOutputStream(orderLinePath));
                outNewOrder = new PrintWriter(new FileOutputStream(newOrderPath));
                LOG.debug("\nWriting Order file to: " + orderPath);
                LOG.debug("\nWriting Order Line file to: " + orderLinePath);
                LOG.debug("\nWriting New Order file to: " + newOrderPath);
            }

            now = new java.util.Date();
            Oorder oorder = new Oorder();
            NewOrder new_order = new NewOrder();
            OrderLine order_line = new OrderLine();
            jdbcIO myJdbcIO = new jdbcIO();

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
                        // o_carrier_id is set *only* for orders with ids < 2101 [4.3.3.1]
                        if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                            oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);
                        } else {
                            oorder.o_carrier_id = null;
                        }
                        oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);
                        oorder.o_all_local = 1;

                        //**************************************************
                        //TIMESTAMP DENSITY
                        oorder.o_entry_d = clock.populateTick();
                        //**************************************************

                        if (calibrate) {
                            counter.incrementAndGet();
                        }

                        k++;
                        if (!generateCsvFiles) {
                            myJdbcIO.insertOrder(ordrPrepStmt, oorder);
                        } else {
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
                        }

                        // 900 rows in the NEW-ORDER table corresponding to the last
                        // 900 rows in the ORDER table for that district (i.e., with
                        // NO_O_ID between 2,101 and 3,000)

                        if (c >= FIRST_UNPROCESSED_O_ID) {

                            new_order.no_w_id = w;
                            new_order.no_d_id = d;
                            new_order.no_o_id = c;

                            k++;
                            if (!generateCsvFiles) {
                                myJdbcIO.insertNewOrder(nworPrepStmt, new_order);
                            } else {
                                String str = "";
                                str = str + new_order.no_w_id + ",";
                                str = str + new_order.no_d_id + ",";
                                str = str + new_order.no_o_id;
                                outNewOrder.println(str);
                            }
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
                            if (!generateCsvFiles) {
                                myJdbcIO.insertOrderLine(orlnPrepStmt, order_line);
                            } else {
                                String str = "";
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
                            }

                            if ((k % configCommitCount) == 0) {
                                long tmpTime = new java.util.Date().getTime();
                                String etStr = "  Elasped Time(ms): "
                                        + ((tmpTime - lastTimeMS) / 1000.000)
                                        + "                    ";
                                LOG.debug(etStr.substring(0, 30)
                                        + "  Writing record " + k + " of " + t);
                                lastTimeMS = tmpTime;
                                if (!generateCsvFiles) {

                                    ordrPrepStmt.executeBatch();
                                    nworPrepStmt.executeBatch();
                                    orlnPrepStmt.executeBatch();
                                    ordrPrepStmt.clearBatch();
                                    nworPrepStmt.clearBatch();
                                    orlnPrepStmt.clearBatch();

                                    transCommit();
                                }
                            }
                        } // end for [l]
                    } // end for [c]
                } // end for [d]
            } // end for [w]

            LOG.debug("  Writing final records " + k + " of " + t);
            if (!generateCsvFiles) {
                ordrPrepStmt.executeBatch();
                nworPrepStmt.executeBatch();
                orlnPrepStmt.executeBatch();
            } else {
                outLine.close();
                outNewOrder.close();
            }
            transCommit();
            now = new java.util.Date();
            LOG.debug("End Orders Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            se.printStackTrace();
            transRollback();
        } catch (Exception e) {
            e.printStackTrace();
            transRollback();
            if (generateCsvFiles) {
                outLine.close();
                outNewOrder.close();
            }
        }
        return k;

    } // end loadOrder()

    private int loadRegions() throws SQLException {

        int k = 0;
        int t = 0;
        BufferedReader br = null;

        PreparedStatement regionPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_REGION
                + " (r_regionkey, r_name, r_comment) "
                + "VALUES (?, ?, ?)");

        try {
            now = new java.util.Date();
            LOG.debug("\nStart Region Load @ " + now + " ...");
            Region region = new Region();
            File file = new File("src", "pt/haslab/htapbench/configuration/tbl/region_gen.tbl");
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(regionPath));
                LOG.debug("\nWriting Order file to: " + regionPath);
            }

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

                if (generateCsvFiles) {
                    String str = "";
                    str = str + region.r_regionkey + ",";
                    str = str + region.r_name + ",";
                    str = str + region.r_comment;
                    out.println(str);
                } else {
                    regionPrepStmt.setLong(1, region.r_regionkey);
                    regionPrepStmt.setString(2, region.r_name);
                    regionPrepStmt.setString(3, region.r_comment);
                    regionPrepStmt.addBatch();

                    long tmpTime = new java.util.Date().getTime();
                    String etStr = "  Elasped Time(ms): "
                            + ((tmpTime - lastTimeMS) / 1000.000)
                            + "                    ";

                    LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);

                    lastTimeMS = tmpTime;
                    regionPrepStmt.executeBatch();
                    regionPrepStmt.clearBatch();

                    conn.commit();
                }
                line = br.readLine();
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
                    + " of " + t);
            lastTimeMS = tmpTime;

            regionPrepStmt.executeBatch();

            conn.commit();
            now = new java.util.Date();
            LOG.debug("End Region Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return k;

    } // end loadRegions()

    private int loadNations() throws SQLException {

        int k = 0;
        int t = 0;
        BufferedReader br = null;

        PreparedStatement nationPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_NATION
                + " (n_nationkey, n_name, n_regionkey, n_comment) "
                + "VALUES (?, ?, ?, ?)");
        try {

            now = new java.util.Date();
            LOG.debug("\nStart Nation Load @ " + now + " ...");

            Nation nation = new Nation();
            File file = new File("src", "pt/haslab/htapbench/configuration/tbl/nation_gen.tbl");
            br = new BufferedReader(new FileReader(file));
            String line = br.readLine();

            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(nationPath));
                LOG.debug("\nWriting Order file to: " + nationPath);
            }

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

                if (generateCsvFiles) {
                    String str = "";
                    str = str + nation.n_nationkey + ",";
                    str = str + nation.n_name + ",";
                    str = str + nation.n_regionkey + ",";
                    str = str + nation.n_comment;
                    out.println(str);
                } else {
                    nationPrepStmt.setLong(1, nation.n_nationkey);
                    nationPrepStmt.setString(2, nation.n_name);
                    nationPrepStmt.setLong(3, nation.n_regionkey);
                    nationPrepStmt.setString(4, nation.n_comment);
                    nationPrepStmt.addBatch();

                    long tmpTime = new java.util.Date().getTime();
                    String etStr = "  Elasped Time(ms): "
                            + ((tmpTime - lastTimeMS) / 1000.000)
                            + "                    ";
                    LOG.debug(etStr.substring(0, 30)
                            + "  Writing record " + k + " of " + t);
                    lastTimeMS = tmpTime;
                    nationPrepStmt.executeBatch();
                    nationPrepStmt.clearBatch();
                    conn.commit();
                }
                line = br.readLine();
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
                    + " of " + t);
            lastTimeMS = tmpTime;

            conn.commit();
            now = new java.util.Date();
            LOG.debug("End Region Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return k;

    } // end loadNations()

    private int loadSuppliers() throws SQLException {

        int k = 0;
        int t = 0;

        try {
            if (generateCsvFiles) {
                out = new PrintWriter(new FileOutputStream(supplierPath));
                LOG.debug("\nWriting supplier file to: " + supplierPath);
            }

            PreparedStatement supplierPrepStmt = conn.prepareStatement("INSERT INTO " + HTAPBConstants.TABLENAME_SUPPLIER
                    + " (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?)");
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

                if (!generateCsvFiles) {
                    supplierPrepStmt.setLong(1, supplier.su_suppkey);
                    supplierPrepStmt.setString(2, supplier.su_name);
                    supplierPrepStmt.setString(3, supplier.su_address);
                    supplierPrepStmt.setLong(4, supplier.su_nationkey);
                    supplierPrepStmt.setString(5, supplier.su_phone);
                    supplierPrepStmt.setDouble(6, supplier.su_acctbal);
                    supplierPrepStmt.setString(7, supplier.su_comment);
                    supplierPrepStmt.addBatch();

                    if ((k % configCommitCount) == 0) {
                        long tmpTime = new java.util.Date().getTime();
                        String etStr = "  Elasped Time(ms): "
                                + ((tmpTime - lastTimeMS) / 1000.000)
                                + "                    ";
                        LOG.debug(etStr.substring(0, 30)
                                + "  Writing record " + k + " of " + t);
                        lastTimeMS = tmpTime;

                        supplierPrepStmt.executeBatch();
                        supplierPrepStmt.clearBatch();

                        conn.commit();
                    }
                } else {
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
            }

            long tmpTime = new java.util.Date().getTime();
            String etStr = "  Elasped Time(ms): "
                    + ((tmpTime - lastTimeMS) / 1000.000)
                    + "                    ";
            LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
                    + " of " + t);
            lastTimeMS = tmpTime;

            supplierPrepStmt.executeBatch();

            conn.commit();
            now = new java.util.Date();
            if (generateCsvFiles) {
                out.close();
            }
            LOG.debug("End Supplier Data Load @  " + now);

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();
            if (generateCsvFiles) {
                out.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
            if (generateCsvFiles) {
                out.close();
            }
        }

        return k;

    } // end loadSuppliers()

    @Override
    public void load() throws SQLException {
        if (!generateCsvFiles) {
            truncateTable(HTAPBConstants.TABLENAME_ITEM);
            truncateTable(HTAPBConstants.TABLENAME_WAREHOUSE);
            truncateTable(HTAPBConstants.TABLENAME_STOCK);
            truncateTable(HTAPBConstants.TABLENAME_DISTRICT);
            truncateTable(HTAPBConstants.TABLENAME_CUSTOMER);
            truncateTable(HTAPBConstants.TABLENAME_HISTORY);
            truncateTable(HTAPBConstants.TABLENAME_ORDER);
            truncateTable(HTAPBConstants.TABLENAME_ORDERLINE);
            truncateTable(HTAPBConstants.TABLENAME_NEWORDER);
            truncateTable(HTAPBConstants.TABLENAME_NATION);
            truncateTable(HTAPBConstants.TABLENAME_REGION);
            truncateTable(HTAPBConstants.TABLENAME_SUPPLIER);
        }

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
