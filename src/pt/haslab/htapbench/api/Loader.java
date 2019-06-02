
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
package pt.haslab.htapbench.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.catalog.Catalog;
import pt.haslab.htapbench.catalog.Column;
import pt.haslab.htapbench.catalog.Table;
import pt.haslab.htapbench.core.WorkloadConfiguration;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.Histogram;
import pt.haslab.htapbench.util.SQLUtil;

/**
 * Loader class
 */
public abstract class Loader {
    private static final Logger LOG = Logger.getLogger(Loader.class);

    protected final BenchmarkModule benchmark;
    protected Connection conn;
    protected final WorkloadConfiguration workConf;
    protected final double scaleFactor;
    private final Histogram<String> tableSizes = new Histogram<String>(true);

    public Loader(BenchmarkModule benchmark, Connection conn) {
        this.benchmark = benchmark;
        this.conn = conn;
        this.workConf = benchmark.getWorkloadConfiguration();
        this.scaleFactor = workConf.getScaleFactor();
    }

    Histogram<String> getTableCounts() {
        return (this.tableSizes);
    }

    protected DatabaseType getDatabaseType() {
        return (this.workConf.getDBType());
    }

    /**
     * Return the database's catalog
     */
    public Catalog getCatalog() {
        return (this.benchmark.getCatalog());
    }

    /**
     * Get the catalog object for the given table name
     */
    protected Table getTableCatalog(String tableName) {
        Table catalog_tbl = this.benchmark.getCatalog().getTable(tableName.toUpperCase());
        assert (catalog_tbl != null) : "Invalid table name '" + tableName + "'";
        return (catalog_tbl);
    }

    /**
     * Abstract load method
     */
    public abstract void load() throws SQLException;

    /**
     * Method that can be overridden to specifically unload the tables of the
     * database. In the default implementation it checks for tables from the
     * catalog to delete them using SQL. Any subclass can inject custom behavior
     * here.
     *
     * @param catalog The catalog containing all loaded tables
     * @throws SQLException if a SQL related exception occurs
     */
    void unload(Catalog catalog) throws SQLException {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(workConf.getIsolationMode());
        Statement st = conn.createStatement();
        for (Table catalog_tbl : catalog.getTables()) {
            LOG.debug(String.format("Deleting data from %s.%s", workConf.getDBName(), catalog_tbl.getName()));
            String sql = "DELETE FROM " + catalog_tbl.getEscapedName();
            st.execute(sql);
        } // FOR
        conn.commit();
    }
}
