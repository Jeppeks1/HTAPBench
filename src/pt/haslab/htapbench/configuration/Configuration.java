package pt.haslab.htapbench.configuration;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.benchmark.BenchmarkModule;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.configuration.database.MySQL;
import pt.haslab.htapbench.configuration.loader.HTAPBCSVLoader;
import pt.haslab.htapbench.configuration.loader.HTAPBLoader;
import pt.haslab.htapbench.configuration.loader.Loader;
import pt.haslab.htapbench.core.HTAPBenchmark;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.FileUtil;
import pt.haslab.htapbench.util.ScriptRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map.Entry;

import static pt.haslab.htapbench.configuration.Configuration.Mode.*;

public abstract class Configuration {

    private static final Logger LOG = Logger.getLogger(Configuration.class);

    // Configuration variables
    private static BenchmarkModule benchModule;
    private WorkloadConfiguration workConf;
    private HTAPBenchmark benchmark;
    private Connection conn;
    private Statement stmt;
    private Mode mode = Mode.EXECUTE;
    private boolean containsData;

    // Convenience variables
    protected String dbname;
    private String filePathCSV;

    // -------------------------------------------------------------------
    //                        Configuration utility methods
    // -------------------------------------------------------------------

    public Configuration(BenchmarkModule benchModule) {
        this.benchmark = new HTAPBenchmark(benchModule.getWorkloadConfiguration());
        this.filePathCSV = benchModule.getWorkloadConfiguration().getFilePathCSV();
        this.workConf = benchModule.getWorkloadConfiguration();
        this.dbname = benchModule.getBenchmarkName();

        // Check if the database supports explicit transaction control
        if (!getTxnControl())
            throw new UnsupportedOperationException("Option auto-commit = false not yet supported");

        // Set the static member variable
        Configuration.benchModule = benchModule;

        // Save the current Configuration for use in the 'Execution' phase
        benchModule.setConfiguration(this);
    }

    private void createConnection() {
        // Skip the configuration if the mode is 'Generate' as that step is independent of the database
        if (mode == Mode.GENERATE)
            return;

        try {
            // Create a connection and create a statement
            conn = benchModule.makeConnection();
            stmt = conn.createStatement();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Error when trying to connect to the %s server", this), ex);
        }

        // Check if the database already contains data
        checkDataExists();
    }

    private void checkDataExists() {
        // If the flag has already been set, we do not have to check again
        if (containsData)
            return;

        try {
            // Use the ORDER table to determine if the database is empty
            ResultSet rs = stmt.executeQuery("select count(*) from " + HTAPBConstants.TABLENAME_ORDER);

            // Move the cursor to the first row
            rs.next();

            // Check if the table contained any data
            containsData = rs.getLong(1) > 0;
        } catch (SQLException ex) {
            // A SQLException in the above query should indicate the database is empty
            containsData = false;
        }
    }

    /**
     * Prepares the current Worker for executing the benchmark.
     *
     * Each Worker need to create their own connection and point to the
     * database of interest with the database-specific 'use' command. If
     * the database supports the auto-commit mode it should be set along
     * with the desired isolation mode.
     */
    public Connection prepareExecution() {
        try {
            // Open a connection to the database for the calling Worker
            createConnection();

            // Set the connection-specific options
            conn.setAutoCommit(!getTxnControl());
            conn.setTransactionIsolation(workConf.getIsolationMode());

            // Point to the database of interest
            switchDatabase();

            // Return the connection handle
            return conn;
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to connect to database", ex);
        }
    }

    public enum Mode {

        /**
         * Configures the database by creating a database instance
         * if it does not already exist. The database schema is also
         * included as part of this {@code Mode}.
         */
        CONFIGURE,

        /**
         * Generates CSV files to the specified directory or directly
         * loads data into the database using insert statements.
         */
        GENERATE,

        /**
         * Populates the database with data from CSV files or directly
         * through insert statements. For the latter case, this case is
         * equal to the GENERATE mode when the useCSV boolean input
         * parameter is false.
         */
        POPULATE,

        /**
         * The benchmark will be executed when this mode is set.
         */
        EXECUTE,

        /**
         * Parsing the input parameter mode resulted in a parse error
         * and thus an invalid {@code Mode}.
         */
        INVALID
    }

    // -------------------------------------------------------------------
    //                     Database configuration methods
    // -------------------------------------------------------------------

    /**
     * Configures the database by creating a new database and initializing
     * the schema.
     *
     * @param overwrite if {@code true}, overwrites any existing database by
     *                  dropping the database first.
     */
    private void configureDatabase(boolean overwrite) throws SQLException, RuntimeException {
        // Skip the configuration if the mode is 'Generate' as that step is independent of the database
        if (mode == Mode.GENERATE)
            return;

        // Configure a new database from scratch if overwrite = true
        if (overwrite)
            dropDatabase();

        // Create the database if needed and check if a new database instance was created
        createDatabaseIfNotExists();

        // Initialize the schema if the database does not contain data
        if (!containsData) {
            initSchema();
        }
    }

    /**
     * This method generates CSV data for the benchmark.
     * <p>
     * The default behaviour of this method is to never overwrite any existing data,
     * whether that be CSV files or already loaded data from previous benchmarks.
     * The only scenario where existing CSV files or database data are overwritten is
     * if overwrite = true.
     */
    private void generateCSVData(boolean overwrite) throws SQLException {
        // Create an instance of the CSV loader class
        Loader loader = new HTAPBCSVLoader(benchmark);
        loader.setNullConstant(getNull());

        // Create an empty directory at the specified path and check if
        // the path is now empty after the method invocation.
        boolean emptyPath = FileUtil.makeDirIfNotExists(workConf.getFilePathCSV());

        try {
            // Only write the CSV files if the current mode is 'Generate' and the
            // overwrite flag is set or if the path was empty.
            if (mode == Mode.GENERATE && overwrite || emptyPath)
                loader.load();

        } catch (SQLException ex) {
            throw new SQLException(String.format("Unexpected error when trying to generate data for the %s database", dbname), ex);
        }
    }

    /**
     * Populates the database through a bulk insert of CSV files or through insert
     * statements issued directly to the database.
     */
    private void populateDatabase(boolean overwrite) throws SQLException {
        // Make sure we are not overwriting existing data without permission
        if (containsData && !overwrite) {
            LOG.info("Skipped database population: Database contains data");
            return;
        } else {
            LOG.info("Beginning database population ... This may take a while");
        }

        // Check if all the prerequisites have been met before proceeding
        checkPrerequisites(Mode.POPULATE);

        // Populate the database with insert statements or through CSV files
        if (!workConf.getUseCSV()) {
            // Create an instance of the insert-based Loader class
            Loader loader = new HTAPBLoader(benchmark, conn);
            loader.setNullConstant(getNull());

            // Populate the database through insert statements
            loader.load();
        } else {
            // Generate the CSV files if they do not already exist
            generateCSVData(overwrite);

            // Bulk load the data into the database using the database-specific command
            for (Entry<String, String> entry : HTAPBConstants.tableToCSV.entrySet()) {
                String tableName = filePathCSV + "/" + entry.getValue();
                stmt.executeQuery(String.format(getBulk(), tableName, entry.getKey()));
                LOG.debug("Populated the " + entry.getKey() + " table");
            }
        }

        // The database now contains data
        containsData = true;

        // Log the progress
        LOG.info("Succesfully populated the database");

    }

    // -------------------------------------------------------------------
    //                     Database configuration helper functions
    // -------------------------------------------------------------------

    private void createDatabaseIfNotExists() throws SQLException {
        try {
            // Create the database if it does not exist
            stmt.execute(getCreate());

            // Switch to the database that is now guaranteed to exist
            switchDatabase();

            // Check if the database is a new instance or not
            checkDataExists();

            // Write the information to the log
            if (containsData)
                LOG.info("The " + dbname + " database contains data from a previous execution");
            else
                LOG.info("A new empty " + dbname + " database was created");
        } catch (SQLException ex) {
            throw new SQLException(String.format("Error when trying to create the %s database in %s", dbname, this), ex);
        }
    }

    /**
     * This accessory method creates a database from the DDL script that should be placed in
     * /configuration/ddl with the naming convention dbname-dbType-ddl.sql where dbname and
     * dbType are two inputs to this method.
     * <p>
     * The purpose of this method is to provide the Catalog an opportunity to initialize
     * an in-memory HSQL database where metadata about the test database is stored.
     *
     * @param dbType The DatabaseType of the database for which the DDL file should be run.
     * @param conn   A handle to the already open connection for the dbType instance.
     * @param dbname The name of the test database - usually htapb.
     */
    public static void createDatabaseFromDDL(DatabaseType dbType, Connection conn, String dbname) {
        try {
            URL ddl = Configuration.getDatabaseDDL(dbType, dbname);
            assert (ddl != null) : "Failed to get DDL for " + dbType.name();
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            if (LOG.isDebugEnabled()) LOG.debug("Executing script '" + ddl + "'");
            runner.runScript(ddl);
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to create the %s database", dbname), ex);
        }
    }

    private void switchDatabase() {
        try {
            stmt.execute(getUse());
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Error when trying to switch to the %s database in %s", dbname, this), ex);
        }

        // Check if the database already contains data
        checkDataExists();
    }

    private void dropDatabase() throws SQLException {
        try {
            stmt.execute(getDrop());
            containsData = false;
        } catch (SQLException ex) {
            throw new SQLException(String.format("Error when trying to drop the %s database in %s", dbname, this), ex);
        }

        LOG.info("Succesfully dropped the " + dbname + " database");
    }

    /**
     * Initialize the Benchmark Configuration. This is the main method used to
     * create all the database objects (e.g., table, indexes, etc) needed
     * for this benchmark.
     */
    private void initSchema() throws RuntimeException {
        try {
            URL ddl = getDatabaseDDL();
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            runner.runScript(ddl);
        } catch (SQLException | IOException ex) {
            throw new RuntimeException("Error when trying to initialize the database schema", ex);
        }

        LOG.info("Initialized the database schema");
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database schema.
     */
    private URL getDatabaseDDL() throws FileNotFoundException {
        // Get the URL of the DDL file belonging to this database
        String db_type = this.toString().toLowerCase();
        String ddlName = dbname + "-" + db_type + "-ddl.sql";
        URL ddl = Configuration.class.getResource("ddl/" + ddlName);

        // Check if the DDL file was found
        if (ddl == null)
            throw new FileNotFoundException("Failed to get DDL file " + ddlName + " for " + this);

        return ddl;
    }

    /**
     * Return the URL handle to the DDL used to load the benchmark's database schema.
     * This method provides a static entry-point for classes that need access to a ddl
     * file, but does not have a Configuration in scope.
     *
     * @param db_type the database type.
     * @param dbname  the name of the database instance.
     * @return a URL pointing to the location of the ddl file.
     * @throws FileNotFoundException if the ddl file was not found.
     */
    public static URL getDatabaseDDL(DatabaseType db_type, String dbname) throws FileNotFoundException {
        // Create the ddl filename for the given input
        String dbtype = db_type != null ? db_type.name().toLowerCase() : "";
        String ddlName = dbname + "-" + dbtype + "-ddl.sql";

        // Retrieve the URL for the ddl
        URL ddl = Configuration.class.getResource("ddl/" + ddlName);

        // Check if the DDL file was found
        if (ddl == null)
            throw new FileNotFoundException("Failed to get DDL file " + ddlName + " for " + dbtype);

        return ddl;
    }

    // -------------------------------------------------------------------
    //                        Ensure prerequisites
    // -------------------------------------------------------------------

    /**
     * Checks if the prerequisites for the given mode are met.
     *
     * @param mode the {@code Mode} for which to check if the operation can continue.
     */
    private void checkPrerequisites(Mode mode) {
        // Check if the prerequisites for the given mode are met
        if (mode == Mode.POPULATE) {
            // Check if the database-specific prerequisites for the population phase are met.
            try {
                if (this instanceof MySQL) {
                    // Check if the user has permission to use the local infile command
                    ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'local_infile'");

                    // Move the cursor to the first row
                    rs.next();

                    // Check if the command is possible for the current user
                    String localInfile = rs.getString(2);
                    if (!(localInfile.equalsIgnoreCase("on") || localInfile.equalsIgnoreCase("1")))
                        throw new SQLException("User does not have permission to use the local infile command " +
                                "or the option has not been set as a system-wide setting. Please check the settings " +
                                "or perform the population manually.");
                }
            } catch (SQLException ex) {
                throw new RuntimeException(String.format("Error when checking POPULATE prerequisites for the %s database in %s", dbname, this), ex);
            }
        } else if (mode == Mode.EXECUTE) {
            // Point to the database of interest
            switchDatabase();

            // There should now be a database containing data
            checkDataExists();

            if (!containsData)
                throw new RuntimeException(String.format("Error when checking EXECUTE prerequisites for the %s database in %s. " +
                        "No data was detected in the database.", dbname, this));
        }
    }


    // -------------------------------------------------------------------
    //                        Abstract methods
    // -------------------------------------------------------------------

    /**
     * The database-specific boolean value indicating if the database
     * supports explicit transaction control through the commit method.
     * If the value is set to {@code false}, the explicit call to commit()
     * and rollback() will be avoided and the database must decide whether
     * to commit or rollback on its own.
     *
     * @return boolean value indicating if the database supports explicit
     * transaction control.
     */
    public abstract boolean getTxnControl();

    /**
     * The database-specific command to create a named database if the database
     * does not already exist. Should include a placeholder for the database name.
     *
     * @return command to create a new database.
     */
    public abstract String getCreate();

    /**
     * The database-specific command to drop a named database if it exists.
     * Should include a placeholder for the database name.
     *
     * @return command to drop a database.
     */
    public abstract String getDrop();

    /**
     * The database-specific command to bulk insert a CSV file
     * into a named table. Include placeholders for the path
     * and then the table name.
     *
     * @return command to bulk insert from a CSV file.
     */
    public abstract String getBulk();

    /**
     * The database-specific command to switch to another database.
     * Should include placeholder for a named database.
     *
     * @return command to switch to another database.
     */
    public abstract String getUse();

    /**
     * The database-specific null-constant so that the insert commands
     * recognizes a String as intended to be null. An example includes
     * the MySQL null-constant "\\N".
     *
     * @return the database-specific null-constant.
     */
    public abstract String getNull();


    // -------------------------------------------------------------------
    //                Public methods for Configuration configuration
    // -------------------------------------------------------------------

    /**
     * Configures the Configuration according to the specified mode:
     * <p><p>
     * 'Configure' creates a new database and initializes the schema.
     * <p>
     * 'Generate' writes the CSV files to the directory 'filePath'.
     * <p>
     * 'Populate' loads the data into the tables of the database.
     * <p>
     * 'Execute' begins the execution phase of the benchmark.
     * <p><p>
     * The behaviour of this method can be adjusted using the overwrite parameter.
     * <p> See its description for the effect.
     *
     * @param modeInput indicates the configuration strategy of the database instance.
     * @param overwrite if {@code true}, the current mode will forcefully overwrite
     *                  the current database or CSV data, effectively dropping and
     *                  re-populating the database in order to prepare for the next
     *                  benchmark and begin the test from scratch.
     * @param sequence if {@code false}, only the given input mode will be executed.
     * @throws SQLException if an exception occurs when operating on the database
     */
    public void prepareDatabase(Mode modeInput, boolean overwrite, boolean sequence) throws SQLException {
        // Set the mode variable so that other methods can access it
        mode = modeInput;

        try {
            // Create a connection to the database
            createConnection();

            if (sequence){
                // The 'Configure' step should always be the first step
                configureDatabase(overwrite);

                // Fall-through until we reach the given mode
                if (mode == CONFIGURE)
                    return;

                // Generate the data files if necessary
                generateCSVData(overwrite);

                // Fall-through until we reach the given mode
                if (mode == GENERATE)
                    return;

                // Populate the database if required - the database is ready for
                // testing after this method has been invoked.
                populateDatabase(overwrite);
            } else {
                if (mode == CONFIGURE)
                    configureDatabase(overwrite);
                else if (mode == GENERATE)
                    generateCSVData(overwrite);
                else if (mode == POPULATE)
                    populateDatabase(overwrite);
            }

            // Check if everything went well before executing
            if (mode == EXECUTE)
                checkPrerequisites(Mode.EXECUTE);
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    /**
     * Run a script on a Configuration
     */
    public void runScript(String script) {
        try {
            ScriptRunner runner = new ScriptRunner(conn, true, true);
            File scriptFile = new File(script);
            runner.runScript(scriptFile.toURI().toURL());
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to run: %s", script), ex);
        } catch (IOException ex) {
            throw new RuntimeException(String.format("Unexpected error when trying to open: %s", script), ex);
        }
    }

    public static Configuration instantiate(BenchmarkModule benchmarkModule) throws UnsupportedOperationException {
        // Get the DatabaseType for the given BenchmarkModule
        DatabaseType dbtype = benchmarkModule.getWorkloadConfiguration().getDBType();

        // Instantiate the appropriate Configuration class
        switch (dbtype) {
            case MYSQL:
                return new MySQL(benchmarkModule);
            default:
                throw new UnsupportedOperationException("Support for automatic configuration of database " + dbtype + " not implemented. " +
                        "Please prepare the database configuration manually through the command line and " +
                        "then execute the benchmark with sequence = false.");
        }
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}

