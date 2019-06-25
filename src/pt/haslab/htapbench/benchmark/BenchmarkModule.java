
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
package pt.haslab.htapbench.benchmark;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.api.*;
import pt.haslab.htapbench.api.dialects.StatementDialects;
import pt.haslab.htapbench.catalog.Catalog;

import pt.haslab.htapbench.configuration.Configuration;
import pt.haslab.htapbench.core.TPCHWorker;
import pt.haslab.htapbench.core.Worker;
import pt.haslab.htapbench.core.Clock;
import pt.haslab.htapbench.core.Workload;
import pt.haslab.htapbench.util.ClassUtil;

/**
 * Base class for all benchmark implementations
 */
public abstract class BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(BenchmarkModule.class);

    private final String benchmarkName;

    /**
     * The workload configuration for this benchmark invocation
     */
    protected WorkloadConfiguration workConf;

    /**
     * These are the variations of the Procedure's Statment SQL
     */
    protected final StatementDialects dialects;

    /**
     * The database-specific configuration instance.
     */
    private Configuration config = null;

    /**
     * The Workload for the current benchmark.
     */
    private Workload workload;

    /**
     * Database Catalog
     */
    protected final Catalog catalog;

    /**
     * Supplemental Procedures
     */
    private final Set<Class<? extends Procedure>> supplementalProcedures = new HashSet<Class<? extends Procedure>>();

    /**
     * A single Random object that should be re-used by all a benchmark's components
     */
    private final Random rng = new Random();

    /**
     * Whether to use verbose output messages
     *
     * @deprecated
     */
    protected boolean verbose;

    public BenchmarkModule(String benchmarkName, WorkloadConfiguration workConf, boolean withCatalog) {
        assert (workConf != null) : "The WorkloadConfiguration instance is null.";

        this.benchmarkName = benchmarkName;
        this.workConf = workConf;
        this.catalog = (withCatalog ? new Catalog(this) : null);
        File xmlFile = this.getSQLDialect();
        this.dialects = new StatementDialects(this.workConf.getDBType(), xmlFile);
    }

    // -------------------------------------------------------------------
    //                         Database connection
    // -------------------------------------------------------------------

    public final Connection makeConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(workConf.getDBConnection(),
                workConf.getDBUsername(),
                workConf.getDBPassword());
        Catalog.setSeparator(conn);
        return (conn);
    }

    // -------------------------------------------------------------------
    //                     Implementing class interface
    // -------------------------------------------------------------------

    protected abstract List<Worker> makeWorkersImpl(String workerType);

    protected abstract TPCHWorker makeOneOLAPWorkerImpl();

    /**
     * Each BenchmarkModule needs to implement this method to load a sample
     * dataset into the database. The Connection handle will already be
     * configured for you, and the base class will commit+close it once this
     * method returns
     */

    protected abstract Package getProcedurePackageImpl();

    protected abstract Package getProcedurePackageImpl(String txnName);

    // -------------------------------------------------------------------
    //                         Public interface
    // -------------------------------------------------------------------

    /**
     * Return the Random generator that should be used by all this benchmark's components
     */
    public Random rng() {
        return (this.rng);
    }

    /**
     * Return the File handle to the SQL Dialect XML file used for this benchmark
     */
    private File getSQLDialect() {
        String xmlName = this.benchmarkName + "-dialects.xml";
        URL ddlURL = this.getClass().getResource(xmlName);
        if (ddlURL != null) return new File(ddlURL.getPath());
        if (LOG.isDebugEnabled())
            LOG.warn(String.format("Failed to find SQL Dialect XML file '%s'", xmlName));
        return (null);
    }

    /**
     * Calls the implementation for making all the OLTP Workers.
     */
    public final List<Worker> makeWorkers(String workerType) {
        return (this.makeWorkersImpl(workerType));
    }

    /**
     * Calls the implementation for making a OLAP Worker.
     */
    public final TPCHWorker makeOLAPWorker() {
        return this.makeOneOLAPWorkerImpl();
    }

    // --------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------

    /**
     * Return the Configuration used to configure the database.
     */
    public final Configuration getConfiguration() {
        return this.config;
    }

    /**
     * Set the Configuration used to configure the database.
     */
    public final void setConfiguration(Configuration config) {
        this.config = config;
    }


    /**
     * Return the unique identifier for this benchmark
     */
    public final String getBenchmarkName() {
        return (this.benchmarkName);
    }

    /**
     * Return the database's catalog
     */
    public final Catalog getCatalog() {
        return (this.catalog);
    }

    /**
     * Return the StatementDialects loaded for this benchmark
     */
    public final StatementDialects getStatementDialects() {
        return (this.dialects);
    }

    @Override
    public final String toString() {
        return benchmarkName.toUpperCase();
    }

    /**
     * Initialize a TransactionType handle for the get procedure name and id.
     * This should only be invoked a start-up time
     */
    @SuppressWarnings("unchecked")
    public final TransactionType initTransactionType(String procName, int id) {
        if (id == TransactionType.INVALID_ID) {
            LOG.error(String.format("Procedure %s.%s cannot the reserved id '%d' for %s",
                    this.benchmarkName, procName, id,
                    TransactionType.INVALID.getClass().getSimpleName()));
            return null;
        }

        //Package pkg = this.getProcedurePackageImpl();
        Package pkg = this.getProcedurePackageImpl(procName);
        assert (pkg != null) : "Null Procedure package for " + this.benchmarkName;
        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(fullName);
        assert (procClass != null) : "Unexpected Procedure name " + this.benchmarkName + "." + procName;
        return new TransactionType(procClass, id);
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.workConf);
    }

    /**
     * Return a mapping from TransactionTypes to Procedure invocations
     */
    public Map<TransactionType, Procedure> getProcedures() {
        Map<TransactionType, Procedure> proc_xref = new HashMap<TransactionType, Procedure>();
        TransactionTypes txns = this.workConf.getTransTypes();

        if (txns != null) {
            for (Class<? extends Procedure> procClass : this.supplementalProcedures) {
                TransactionType txn = txns.getType(procClass);
                if (txn == null) {
                    txn = new TransactionType(procClass, procClass.hashCode(), true);
                    txns.add(txn);
                }
            } // FOR

            for (TransactionType txn : txns) {
                Procedure proc = ClassUtil.newInstance(txn.getProcedureClass(),
                        new Object[0],
                        new Class<?>[0]);
                proc.initialize(this.workConf.getDBType());
                proc_xref.put(txn, proc);
                proc.loadSQLDialect(this.dialects);
            } // FOR
        }
        if (proc_xref.isEmpty()) {
            LOG.warn("No procedures defined for " + this);
        }
        return (proc_xref);
    }

    public void setWorkloadConfiguration(WorkloadConfiguration workConf) {
        this.workConf = workConf;
    }
}
