
/**
 * Copyright 2015 by OLTPBenchmark Project                                   *
 * *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 * *
 * http://www.apache.org/licenses/LICENSE-2.0                              *
 * *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 * *****************************************************************************
 * /*
 * Copyright 2017 by INESC TEC
 * This work was based on the OLTPBenchmark Project
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author pavlo
 */
/**
 *
 * @author pavlo
 */
package pt.haslab.htapbench.configuration.loader;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.benchmark.BenchmarkModule;
import pt.haslab.htapbench.catalog.Catalog;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.core.Clock;
import pt.haslab.htapbench.core.HTAPBenchmark;
import pt.haslab.htapbench.random.RandomParameters;

import static pt.haslab.htapbench.benchmark.HTAPBConstants.*;

/**
 * Loader class
 */
public abstract class Loader {
    private static final Logger LOG = Logger.getLogger(Loader.class);

    final WorkloadConfiguration workConf;
    protected final BenchmarkModule benchmark;
    protected final RandomParameters randomParam;
    protected final Clock clock;
    protected Connection conn;
    String nullConstant;

    final boolean generateCsvFiles;
    final boolean calibrate;
    final double scaleFactor;
    final String csvFilePath;

    // CSV file names
    final String warehousePath;
    final String orderLinePath;
    final String newOrderPath;
    final String customerPath;
    final String districtPath;
    final String supplierPath;
    final String historyPath;
    final String nationPath;
    final String regionPath;
    final String stockPath;
    final String orderPath;
    final String itemPath;

    Loader(HTAPBenchmark benchmark, Connection conn) {
        this.workConf = benchmark.getWorkloadConfiguration();
        this.generateCsvFiles = workConf.getUseCSV();
        this.csvFilePath = workConf.getFilePathCSV();
        this.scaleFactor = workConf.getScaleFactor();
        this.calibrate = workConf.getCalibrate();
        this.benchmark = benchmark;
        this.conn = conn;

        // Set the CSV file names
        this.warehousePath = csvFilePath + "/" + CSVNAME_WAREHOUSE;
        this.orderLinePath = csvFilePath + "/" + CSVNAME_ORDERLINE;
        this.newOrderPath = csvFilePath + "/" + CSVNAME_NEWORDER;
        this.customerPath = csvFilePath + "/" + CSVNAME_CUSTOMER;
        this.districtPath = csvFilePath + "/" + CSVNAME_DISTRICT;
        this.supplierPath = csvFilePath + "/" + CSVNAME_SUPPLIER;
        this.historyPath = csvFilePath + "/" + CSVNAME_HISTORY;
        this.nationPath = csvFilePath + "/" + CSVNAME_NATION;
        this.regionPath = csvFilePath + "/" + CSVNAME_REGION;
        this.stockPath = csvFilePath + "/" + CSVNAME_STOCK;
        this.orderPath = csvFilePath + "/" + CSVNAME_ORDER;
        this.itemPath = csvFilePath + "/" + CSVNAME_ITEM;

        // Set the clock
        String csvFilePath = workConf.getFilePathCSV();
        this.clock = new Clock((int) scaleFactor, csvFilePath);

        // Initialize the RandomParameter
        this.randomParam = new RandomParameters("uniform", (int) scaleFactor);
    }

    /**
     * Return the database's catalog
     */
    public Catalog getCatalog() {
        return (this.benchmark.getCatalog());
    }


    /**
     * Abstract load method
     */
    public abstract void load() throws SQLException;

    /**
     * Set the null constant to use so that the database recognizes a String
     * to be the SQL-null value.
     *
     * @param nullConstant the null-constant string.
     */
    public void setNullConstant(String nullConstant){
        this.nullConstant = nullConstant;
    }
}
