
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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;

import pt.haslab.htapbench.configuration.Configuration.Mode;
import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.core.BenchmarkState;
import pt.haslab.htapbench.core.Phase;
import pt.haslab.htapbench.core.WorkloadState;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.FileUtil;
import pt.haslab.htapbench.util.StringUtil;

public class WorkloadConfiguration {

    // -------------------------------------------------------------------

    //                         Private variables
    // -------------------------------------------------------------------

    private List<Phase> works = new ArrayList<Phase>();
    private TransactionTypes transTypes = null;
    private TraceReader traceReader = null;
    private WorkloadState workloadState;
    private DatabaseType db_type;

    private String benchmarkName;
    private String db_connection;
    private String filePathCSV;
    private String db_username;
    private String db_password;
    private String db_driver;
    private String db_name;

    private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
    private int numberOfPhases = 0;
    private int intervalMonitor = 0;
    private int OLAPterminals;
    private int numTxnTypes;
    private int targetTPS;
    private int terminals;

    private boolean useCSV = true;
    private boolean recordAbortMessages;
    private boolean hybridWorkload;
    private boolean idealClient;
    private boolean calibrate;

    private double scaleFactor = 1.0;
    private double errorMargin = 0.2;

    // -------------------------------------------------------------------
    //                        Configuration methods
    // -------------------------------------------------------------------

    /**
     * Initiate a new benchmark and workload state.
     */
    public WorkloadState initializeState(BenchmarkState benchmarkState) {
        assert (workloadState == null);
        workloadState = new WorkloadState(benchmarkState, works, terminals, traceReader);
        return workloadState;
    }

    /**
     * Wraps the arguments in a Phase, which defines the workload and stages
     * it for execution by adding it to the list of Phases to be executed.
     */
    public void addWork(int time, int rate, List<String> weights, boolean rateLimited, boolean disabled, boolean serial, boolean timed, int active_terminals, Phase.Arrival arrival) {
        works.add(new Phase(benchmarkName, numberOfPhases, time, rate, weights, rateLimited, disabled, serial, timed, active_terminals, arrival));
        numberOfPhases++;
    }

    /**
     * A utility method that inits the phaseIterator and dialectMap.
     */
    public void init() {
        try {
            Class.forName(this.db_driver);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException("Failed to initialize JDBC driver '" + this.db_driver + "'", ex);
        }
    }

    // -------------------------------------------------------------------
    //                            Only getters
    // -------------------------------------------------------------------

    // WorkloadState
    public WorkloadState getWorkloadState() {
        return workloadState;
    }

    // Number of phases
    public int getNumberOfPhases() {
        return this.numberOfPhases;
    }

    // All recorded phases
    public List<Phase> getAllPhases() {
        return works;
    }

    // -------------------------------------------------------------------
    //                         Getters and Setters
    // -------------------------------------------------------------------

    // TraceReader
    public TraceReader getTraceReader() {
        return traceReader;
    }

    public void setTraceReader(TraceReader traceReader) {
        this.traceReader = traceReader;
    }

    // BenchmarkName
    public String getBenchmarkName() {
        return benchmarkName;
    }

    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    // DatabaseType
    public void setDBType(DatabaseType dbType) {
        db_type = dbType;
    }

    public DatabaseType getDBType() {
        return db_type;
    }

    // Database connection
    public void setDBConnection(String database) {
        this.db_connection = database;
    }

    public String getDBConnection() {
        return db_connection;
    }

    // Database name
    public String getDBName() {
        return db_name;
    }

    public void setDBName(String dbname) {
        this.db_name = dbname;
    }

    // Number of transaction types
    public int getNumTxnTypes() {
        return numTxnTypes;
    }

    public void setNumTxnTypes(int numTxnTypes) {
        this.numTxnTypes = numTxnTypes;
    }

    // Database username
    public void setDBUsername(String username) {
        this.db_username = username;
    }

    String getDBUsername() {
        return db_username;
    }

    // Database password
    public void setDBPassword(String password) {
        this.db_password = password;
    }

    String getDBPassword() {
        return this.db_password;
    }

    // Target TPS
    public void setTargetTPS(int targetTPS) {
        this.targetTPS = targetTPS;
    }

    public int getTargetTPS() {
        return this.targetTPS;
    }

    // Ideal client
    public void setIdealClient(boolean flag) { this.idealClient = flag;}

    public boolean getIdealClient() {return this.idealClient;}

    // Database driver
    public void setDBDriver(String driver) {
        this.db_driver = driver;
    }

    public String getDBDriver() {
        return this.db_driver;
    }

    // Calibration value
    public boolean getCalibrate() {
        return this.calibrate;
    }

    public void setCalibrate(Mode mode) {
        this.calibrate = mode != Mode.EXECUTE;
    }

    // Hybrid workload
    public boolean getHybridWorkload() { return this.hybridWorkload; }

    public void setHybridWorkload(boolean flag) { this.hybridWorkload = flag; }

    // Error margin
    public double getErrorMargin() {
        return this.errorMargin;
    }

    public void setErrorMargin(double errorMargin) {
        this.errorMargin = errorMargin;
    }

    // Interval monitoring
    public int getIntervalMonitor() {
        return this.intervalMonitor;
    }

    public void setIntervalMonitor(int intervalMonitor) {
        this.intervalMonitor = intervalMonitor;
    }

    // Generate CSV files boolean
    public boolean getUseCSV() {
        return this.useCSV;
    }

    public void setUseCSV(boolean bool) {
        this.useCSV = bool;
    }

    // CSV files path
    public String getFilePathCSV() {
        return this.filePathCSV;
    }

    public void setFilePathCSV(String path) {
        this.filePathCSV = FileUtil.resolvePath(path);
    }

    // Number of TPCC terminals
    public int getTerminals() {
        return terminals;
    }

    public void setTerminals(int terminals) {
        this.terminals = terminals;
    }

    // Number of OLAP terminals
    public int getOLAPTerminals() {
        return OLAPterminals;
    }

    public void setOLAPTerminals(int terminals) {
        this.OLAPterminals = terminals;
    }

    // Transaction types
    public TransactionTypes getTransTypes() {
        return transTypes;
    }

    public void setTransTypes(TransactionTypes transTypes) {
        this.transTypes = transTypes;
    }

    /**
     * Whether each worker should record the transaction's UserAbort messages
     * This is primarily useful for debugging a benchmark
     */
    public boolean getRecordAbortMessages() {
        return (this.recordAbortMessages);
    }

    public void setRecordAbortMessages(boolean recordAbortMessages) {
        this.recordAbortMessages = recordAbortMessages;
    }

    /**
     * Return the scale factor of the database size
     */
    public double getScaleFactor() {
        return this.scaleFactor;
    }

    /**
     * Set the scale factor for the database
     * A value of 1 means the default size.
     * A value greater than 1 means the database is larger
     * A value less than 1 means the database is smaller
     */
    public void setScaleFactor(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    // Isolation mode
    public int getIsolationMode() {
        return isolationMode;
    }

    public void setIsolationMode(String mode) {
        if (mode.equals("TRANSACTION_SERIALIZABLE"))
            this.isolationMode = Connection.TRANSACTION_SERIALIZABLE;
        else if (mode.equals("TRANSACTION_READ_COMMITTED"))
            this.isolationMode = Connection.TRANSACTION_READ_COMMITTED;
        else if (mode.equals("TRANSACTION_REPEATABLE_READ"))
            this.isolationMode = Connection.TRANSACTION_REPEATABLE_READ;
        else if (mode.equals("TRANSACTION_READ_UNCOMMITTED"))
            this.isolationMode = Connection.TRANSACTION_READ_UNCOMMITTED;
        else if (!mode.isEmpty())
            System.out.println("Indefined isolation mode, set to default [TRANSACTION_SERIALIZABLE]");
    }

    public String getIsolationString() {
        if (this.isolationMode == Connection.TRANSACTION_SERIALIZABLE)
            return "TRANSACTION_SERIALIZABLE";
        else if (this.isolationMode == Connection.TRANSACTION_READ_COMMITTED)
            return "TRANSACTION_READ_COMMITTED";
        else if (this.isolationMode == Connection.TRANSACTION_REPEATABLE_READ)
            return "TRANSACTION_REPEATABLE_READ";
        else if (this.isolationMode == Connection.TRANSACTION_READ_UNCOMMITTED)
            return "TRANSACTION_READ_UNCOMMITTED";
        else
            return "TRANSACTION_SERIALIZABLE [DEFAULT]";
    }

    // To string
    @Override
    public String toString() {
        Class<?> confClass = this.getClass();
        Map<String, Object> m = new ListOrderedMap<String, Object>();

        // Get all the fields of this WorkloadConfiguration
        for (Field f : confClass.getDeclaredFields()) {
            Object obj;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }

            m.put(f.getName().toUpperCase(), obj);
        }

        // Print each field in the map
        return StringUtil.formatMaps(m);
    }
}
