
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
package pt.haslab.htapbench.core;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import pt.haslab.htapbench.api.BenchmarkModule;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.benchmark.HTAPBenchmark;
import pt.haslab.htapbench.densitity.Clock;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.FileUtil;
import pt.haslab.htapbench.util.ResultUploader;
import pt.haslab.htapbench.util.StringUtil;
import pt.haslab.htapbench.util.TimeUtil;

public class HTAPBench {

    private static final Logger LOG = Logger.getLogger(HTAPBench.class);
    private static final String SINGLE_LINE = "**********************************************************************************";

    // This list is used for filtering of the output in case there are inactive transaction types
    private static List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();

    // A list of BenchmarkModules is used to possibly run a benchmark test with more than one Phase
    private static List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();

    private static String generateFilesPath;
    private static boolean idealClient;

    public static void main(String[] args) throws Exception {
        // Initialize log4j
        String log4jPath = "./log4j.configuration";
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }

        // Create the command line parser and XMLConfiguration for reading the plugin name
        CommandLineParser parser = new PosixParser();
        XMLConfiguration pluginConfig = null;

        try {
            // The plugin configuration is used as a naming convention for database related files
            pluginConfig = new XMLConfiguration("./config/plugin.xml");
            pluginConfig.setExpressionEngine(new XPathExpressionEngine());
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }

        // -------------------------------------------------------------------
        //               Handle the command line arguments
        // -------------------------------------------------------------------

        Options options = new Options();
        options.addOption("b", "bench", true, "[required] Benchmark class. Currently supported: " + pluginConfig.getList("/plugin/@name"));
        options.addOption("c", "config", true, "[required] Workload configuration file");
        options.addOption(null, "create", true, "Initialize the database for this benchmark");
        options.addOption(null, "clear", true, "Clear all records in the database for this benchmark");
        options.addOption(null, "load", true, "Load data using the benchmark's data loader");
        options.addOption(null, "generateFiles", true, "Generate CSV Files to Populate Database");
        options.addOption(null, "filePath", true, "Path to generate the CSV files to load the Database, default is ../csv/?tps");
        options.addOption(null, "execute", true, "Execute the benchmark workload");
        options.addOption(null, "runscript", true, "Run an SQL script");
        options.addOption(null, "upload", true, "Upload the result");
        options.addOption(null, "calibrate", true, "Extracts the parameter densities from the load process.");
        options.addOption(null, "oltp", true, "Runs only the OLTP stage of the benchmark.");
        options.addOption(null, "olap", true, "Runs only the OLAP stage of the benchmark.");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Sampling window");
        options.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in seconds");
        options.addOption("ss", false, "Verbose Sampling per Transaction");
        options.addOption("o", "output", true, "Output filename, default is 'res'");
        options.addOption("d", "directory", true, "Base directory for the result files, default is ./results/?tps");
        options.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
        options.addOption("ts", "tracescript", true, "Script of transactions to execute");
        options.addOption(null, "histograms", false, "Print txn histograms");
        options.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
        options.addOption("ic", "idealClient", false, "Determine the scaling factor based on an ideal client, default false");

        // Parse the command line arguments
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("c")) {
            LOG.fatal("Missing Configuration file");
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("b")) {
            LOG.fatal("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }

        // Get the configuration file
        String configFile = argsLine.getOptionValue("c");
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());

        // Get the filename of the output or otherwise use the default filename 'res'
        String filename = "res";
        if (argsLine.hasOption("o")) {
            filename = argsLine.getOptionValue("o");
        }

        // Store the output directory of the results if given, otherwise use the default "results" directory
        String outputDirectory = "results/" + (int) xmlConfig.getDouble("target_TPS") + "tps";
        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }

        // Retrieve the filePath if the value is set, otherwise use the default data location "csv/"
        generateFilesPath = "../csv/" + (int) xmlConfig.getDouble("target_TPS") + "tps";
        if (argsLine.hasOption("filePath")){
            generateFilesPath = argsLine.getOptionValue("filePath");
        }

        // The timestamp value at the beginning of the test is appended to result files
        String timestampValue = "";
        if (argsLine.hasOption("t")) {
            timestampValue = String.valueOf(TimeUtil.getCurrentTime().getTime()) + "_";
        }

        // The Interval Monitor value determines how often the TPM should be written to the console
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }

        // Check if the idealClient flag has been set, indicating how the workload setup should proceed
        idealClient = argsLine.hasOption("ic");

        // Retrieve the calibrate variable
        boolean calibrate = isBooleanOptionSet(argsLine, "calibrate");

        // Get the error margin allowed for hybrid workloads
        double error_margin = xmlConfig.getDouble("error_margin");

        // -------------------------------------------------------------------
        //                     Benchmark configuration
        // -------------------------------------------------------------------

        // Initialize the WorkloadSetup according to an ideal client
        WorkloadSetup setup = new WorkloadSetup(xmlConfig);
        setup.computeWorkloadSetup(idealClient);

        // Load the configuration for each benchmark in the plugin configuration
        configureBenchmarks(setup, argsLine, configFile, xmlConfig, pluginConfig);

        // -------------------------------------------------------------------
        //                     Command line actions
        // -------------------------------------------------------------------

        // Use the first BenchmarkModule to export dialects if requested
        BenchmarkModule benchDialect = benchList.get(0);
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
            if (benchDialect.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for " + benchDialect);
                String xml = benchDialect.getStatementDialects().export(benchDialect.getWorkloadConfiguration().getDBType(),
                        benchDialect.getProcedures().values());
                System.out.println(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + benchDialect);
        }

        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runCreator(benchmark);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                benchmark.clearDatabase();
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        //Generate Files
        if (isBooleanOptionSet(argsLine, "generateFiles")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Generate Files: " + benchmark.getWorkloadConfiguration().getGeneratesFiles());
                LOG.info("File Path: " + benchmark.getWorkloadConfiguration().getFilesPath());
                runLoader(benchmark, true, true, benchmark.getWorkloadConfiguration().getFilesPath());
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runLoader(benchmark, calibrate, false, "null");
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute a Script
        if (isBooleanOptionSet(argsLine, "runscript")) {
            for (BenchmarkModule benchmark : benchList) {
                String script = argsLine.getOptionValue("runscript");
                LOG.info("Running a SQL script: " + script);
                runScript(benchmark, script);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            List<Results> results = null;

            try {
                if (isBooleanOptionSet(argsLine, "oltp")) {
                    results = runOLTPWorkload(benchList, intervalMonitor);
                } else if (isBooleanOptionSet(argsLine, "olap")) {
                    results = runOLAPWorkload(benchList, intervalMonitor);
                } else {
                    results = runHybridWorkload(benchList, intervalMonitor, error_margin);
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when running benchmark.", ex);
                System.exit(1);
            }

            // -------------------------------------------------------------------
            //                     Collect and print results
            // -------------------------------------------------------------------

            assert (results != null);

            PrintStream ps = System.out;
            PrintStream rs = System.out;

            // Special result uploader
            for (Results r : results) {
                assert (r != null);
                LOG.info(r.getClass());
                ResultUploader ru = new ResultUploader(r, xmlConfig, argsLine);

                // Make the output directory if it does not already exist
                FileUtil.makeDirIfNotExists(outputDirectory);

                // Build the complex path with the requested filename and possibly with the timestamp prepended
                String baseFile = timestampValue + filename;

                // Increment the filename for new results
                String nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));
                ps = new PrintStream(new File(nextName));
                LOG.info("Output into file: " + nextName);

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".raw"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output Raw data into file: " + nextName);

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".summary"));
                PrintStream ss = new PrintStream(new File(nextName));
                LOG.info("Output summary data into file: " + nextName);
                ru.writeSummary(ss);
                ss.close();

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".db.cnf"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output db config into file: " + nextName);
                ru.writeDBParameters(ss);
                ss.close();

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".ben.cnf"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output benchmark config into file: " + nextName);
                ru.writeBenchmarkConf(ss);
                ss.close();

                if (argsLine.hasOption("s")) {
                    int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
                    LOG.info("Grouped into Buckets of " + windowSize + " seconds");
                    r.writeCSV(windowSize, ps);

                    if (isBooleanOptionSet(argsLine, "upload")) {
                        ru.uploadResult();
                    }

                    // Allow more detailed reporting by transaction to make it easier to check
                    if (argsLine.hasOption("ss")) {
                        for (TransactionType t : activeTXTypes) {
                            PrintStream ts = ps;

                            if (ts != System.out) {
                                // Get the actual filename for the output
                                baseFile = baseFile + "_" + t.getName();
                                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));
                                ts = new PrintStream(new File(nextName));
                                r.writeCSV(windowSize, ts, t);
                                ts.close();
                            }
                        }
                    }
                } else if (LOG.isDebugEnabled()) {
                    LOG.warn("No bucket size specified");
                }

                r.writeAllCSVAbsoluteTiming(rs);
            }

            if (argsLine.hasOption("histograms")) {
                Results singleHistogram = results.get(0);

                // Combine the histograms into a single histogram in case of a hybrid workload
                if (results.size() > 1) {
                    // Keep the recordedMessagesHistogram from TPCH results (index 1) over TPCC results (index 0)
                    singleHistogram = results.get(1).combineHistograms(results.get(0));
                }

                LOG.info(SINGLE_LINE);
                if (!singleHistogram.getSuccessHistogram().isEmpty())
                    LOG.info("Completed Transactions:\n" + singleHistogram.getSuccessHistogram() + "\n");

                if (!singleHistogram.getAbortHistogram().isEmpty())
                    LOG.info("Aborted Transactions:\n" + singleHistogram.getAbortHistogram() + "\n");

                if (!singleHistogram.getRetryHistogram().isEmpty())
                    LOG.info("Retried Transactions:\n" + singleHistogram.getRetryHistogram());

                if (!singleHistogram.getErrorHistogram().isEmpty())
                    LOG.info("Unexpected Errors:\n" + singleHistogram.getErrorHistogram());

                if (!singleHistogram.getRecordedMessagesHistogram().isEmpty())
                    LOG.info("Recorded exceptions:\n" + StringUtil.formatRecordedMessages(singleHistogram.getRecordedMessagesHistogram()));
            }

            ps.close();
            rs.close();
        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }

    private static List<String> get_weights(String plugin, SubnodeConfiguration work) {

        @SuppressWarnings("unchecked")
        List<SubnodeConfiguration> weights = work.configurationsAt("weights");
        List<String> weight_strings = new LinkedList<String>();
        boolean weights_started = false;

        for (SubnodeConfiguration weight : weights) {
            // stop if second attributed node encountered
            if (weights_started && weight.getRootNode().getAttributeCount() > 0) {
                break;
            }
            //start adding node values, if node with attribute equal to current plugin encountered
            if (weight.getRootNode().getAttributeCount() > 0 && weight.getRootNode().getAttribute(0).getValue().equals(plugin)) {
                weights_started = true;
            }
            if (weights_started) {
                weight_strings.add(weight.getString(""));
            }
        }

        return weight_strings;
    }

    private static void runScript(BenchmarkModule bench, String script) {
        LOG.debug(String.format("Running %s", script));
        bench.runScript(script);
    }

    private static void runCreator(BenchmarkModule bench) {
        LOG.debug(String.format("Creating %s Database", bench));
        bench.createDatabase();
    }

    private static void runLoader(BenchmarkModule bench, boolean calibrate, boolean generateFiles, String filePath) {
        if (calibrate) {
            LOG.debug(String.format("Loading %s Database for calibration procedure", bench));
        } else {
            LOG.debug(String.format("Loading %s Database", bench));
        }

        bench.loadDatabase(calibrate, generateFiles, filePath);
    }

    private static void configureBenchmarks(WorkloadSetup setup, CommandLine argsLine, String configFile,
                                            XMLConfiguration xmlConfig, XMLConfiguration pluginConfig)
            throws ParseException {
        // Get the benchmark name(s) from the command line
        String targetBenchmarks = argsLine.getOptionValue("b");
        String[] targetList = targetBenchmarks.split(",");

        // Prepare some values to be used in the for loop
        boolean rateLimited;
        int lastTxnId = 0;
        int terminals;

        // Loop through each plugin
        for (String plugin : targetList) {
            String pluginTest = "[@bench='" + plugin + "']";

            // -------------------------------------------------------------------
            //               Initialize the WorkloadConfiguration
            // -------------------------------------------------------------------

            WorkloadConfiguration wrkld = new WorkloadConfiguration();
            wrkld.setBenchmarkName(plugin);
            wrkld.setXmlConfig(xmlConfig);

            boolean scriptRun = false;
            if (argsLine.hasOption("t")) {
                scriptRun = true;
                String traceFile = argsLine.getOptionValue("t");
                wrkld.setTraceReader(new TraceReader(traceFile));
                if (LOG.isDebugEnabled()) LOG.debug(wrkld.getTraceReader().toString());
            }

            // Get the isolation mode from the configuration file
            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");

            // Pull in the database configuration
            wrkld.setDBType(DatabaseType.get(xmlConfig.getString("dbtype")));
            wrkld.setDBDriver(xmlConfig.getString("driver"));
            wrkld.setDBConnection(xmlConfig.getString("DBUrl"));
            wrkld.setDBName(xmlConfig.getString("DBName"));
            wrkld.setDBUsername(xmlConfig.getString("username"));
            wrkld.setDBPassword(xmlConfig.getString("password"));
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setRecordExceptions(xmlConfig.getBoolean("recordExceptions", true));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));
            wrkld.setIdealClient(idealClient);
            wrkld.setFilesPath(generateFilesPath);

            // Use the command line to set the remaining configuration values
            if (isBooleanOptionSet(argsLine, "calibrate")){
                wrkld.setCalibrate(true);
            }

            if (isBooleanOptionSet(argsLine, "generateFiles")) {
                wrkld.setGenerateFiles(Boolean.parseBoolean(argsLine.getOptionValue("generateFiles")));
            }

            if (argsLine.hasOption("filePath")){
                wrkld.setFilesPath(generateFilesPath);
            }

            // Configure based on the requested workload
            if (isBooleanOptionSet(argsLine, "oltp")) {
                wrkld.setScaleFactor(xmlConfig.getDouble("warehouses"));
                terminals = (int) xmlConfig.getDouble("warehouses") * 10;
                wrkld.setTerminals(terminals);
                wrkld.setHybridWrkld(false);
                rateLimited = false;
            } else if (isBooleanOptionSet(argsLine, "olap")) {
                setup.setWarehouses(xmlConfig.getInt("warehouses"));
                wrkld.setScaleFactor(xmlConfig.getDouble("warehouses"));
                terminals = (int) xmlConfig.getDouble("OLAP_workers");
                wrkld.setOLAPTerminals(terminals);
                wrkld.setHybridWrkld(false);
                rateLimited = false;
            } else {
                // HTAP workload
                wrkld.setTerminals(setup.getTerminals());
                terminals = setup.getTerminals();
                wrkld.setOLAPTerminals(1);
                wrkld.setScaleFactor(setup.getWarehouses());
                wrkld.setTargetTPS(setup.getTargetTPS());
                wrkld.setHybridWrkld(true);
                rateLimited = true;
            }

            LOG.debug("");
            LOG.debug("------------- Workload properties --------------------");
            LOG.debug("      Target TPS: " + setup.getTargetTPS());
            LOG.debug("      # warehouses: " + setup.getWarehouses());
            LOG.debug("      #terminals: " + setup.getTerminals());
            LOG.debug("------------------------------------------------------");

            // -------------------------------------------------------------------
            //                     Initialize the BenchmarkModule
            // -------------------------------------------------------------------

            // Create the BenchmarkModule
            BenchmarkModule bench = new HTAPBenchmark(wrkld);

            // Get the classname associated with the current plugin
            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");
            if (classname == null)
                throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");

            // Print the current configuration settings to the console
            Map<String, Object> initDebug = new ListOrderedMap<String, Object>();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDBType());
            initDebug.put("Driver", wrkld.getDBDriver());
            initDebug.put("URL", wrkld.getDBConnection());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            LOG.info(SINGLE_LINE + "\n\n" + StringUtil.formatMaps(initDebug));
            LOG.info(SINGLE_LINE);

            // -------------------------------------------------------------------
            //                     Load the transaction descriptions
            // -------------------------------------------------------------------

            // Get the number of transaction types
            int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();

            // If it is a single workload run, <transactiontypes/> w/o attribute is used
            if (numTxnTypes == 0 && targetList.length == 1) {
                pluginTest = "[not(@bench)]";
                numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            }

            // Set the number of transaction types
            wrkld.setNumTxnTypes(numTxnTypes);

            // Create a container to hold a list of all active transaction types
            List<TransactionType> ttypes = new ArrayList<TransactionType>();
            ttypes.add(TransactionType.INVALID);
            int txnIdOffset = lastTxnId;

            // Parse the configuration file to obtain a list of all active transaction types
            for (int i = 1; i < wrkld.getNumTxnTypes() + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                String txnName = xmlConfig.getString(key + "/name");

                // Get ID if specified; else increment from last one.
                int txnId = i + 1;
                if (xmlConfig.containsKey(key + "/id")) {
                    txnId = xmlConfig.getInt(key + "/id");
                }

                TransactionType tmpType = bench.initTransactionType(txnName, txnId + txnIdOffset);

                // Keep a reference for filtering
                activeTXTypes.add(tmpType);

                // Add a ref for the active TTypes in this benchmark
                ttypes.add(tmpType);
                lastTxnId = i;
            }

            // Wrap the list of transactions and save them
            TransactionTypes tt = new TransactionTypes(ttypes);
            wrkld.setTransTypes(tt);

            // Print the transaction types in case of a debug logging level
            LOG.debug("Using the following transaction types: " + tt);

            // Prepare a container for the possibly grouped transactions
            HashMap<String, List<String>> groupings = new HashMap<String, List<String>>();
            int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();

            // Read in the groupings of transactions (if any) defined for this benchmark
            for (int i = 1; i < numGroupings + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

                // Get the name for the grouping and make sure it's valid.
                String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
                if (!groupingName.matches("^[a-z]\\w*$")) {
                    LOG.fatal(String.format("Grouping name \"%s\" is invalid."
                            + " Must begin with a letter and contain only"
                            + " alphanumeric characters.", groupingName));

                    System.exit(-1);
                } else if (groupingName.equals("all")) {
                    LOG.fatal("Grouping name \"all\" is reserved. Please pick a different name.");
                    System.exit(-1);
                }

                // Get the weights for this grouping and make sure that there is an appropriate number of them.
                List<String> groupingWeights = xmlConfig.getList(key + "/weights");
                if (groupingWeights.size() != numTxnTypes) {
                    LOG.fatal(String.format("Grouping \"%s\" has %d weights,"
                                    + " but there are %d transactions in this"
                                    + " benchmark.", groupingName,
                            groupingWeights.size(), numTxnTypes));

                    System.exit(-1);
                }

                LOG.debug("Creating grouping with name, weights: " + groupingName + ", " + groupingWeights);
                groupings.put(groupingName, groupingWeights);
            }

            // All benchmarks should also have an "all" grouping that gives
            // even weight to all transactions in the benchmark.
            List<String> weightAll = new ArrayList<String>();
            for (int i = 0; i < numTxnTypes; ++i)
                weightAll.add("1");

            groupings.put("all", weightAll);

            // -------------------------------------------------------------------
            //                       More workload configuration
            // -------------------------------------------------------------------

            WorkloadConfiguration workConf = bench.getWorkloadConfiguration();
            workConf.setScaleFactor(setup.getWarehouses());
            workConf.setTerminals(setup.getTerminals());
            workConf.setTargetTPS(setup.getTargetTPS());
            workConf.setFilesPath(generateFilesPath);
            bench.setWorkloadConfiguration(workConf);

            benchList.add(bench);

            // Determine the number of entries in the /works/work section of the configuration
            int size = xmlConfig.configurationsAt("/works/work").size();

            // Loop over the entries, which likely contains <time> and <weights>
            for (int i = 1; i < size + 1; i++) {
                SubnodeConfiguration work = xmlConfig.configurationAt("works/work[" + i + "]");
                List<String> weight_strings;

                // Use a workaround if there multiple workloads or single attributed workload
                if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
                    String weightKey = work.getString("weights" + pluginTest).toLowerCase();
                    if (groupings.containsKey(weightKey))
                        weight_strings = groupings.get(weightKey);
                    else
                        weight_strings = get_weights(plugin, work);
                } else {
                    String weightKey = work.getString("weights[not(@bench)]").toLowerCase();
                    if (groupings.containsKey(weightKey))
                        weight_strings = groupings.get(weightKey);
                    else
                        weight_strings = work.getList("weights[not(@bench)]");
                }

                boolean disabled = false;
                boolean serial = false;
                boolean timed;

                Phase.Arrival arrival = Phase.Arrival.REGULAR;
                String arrive = work.getString("@arrival", "regular");
                if (arrive.toUpperCase().equals("POISSON"))
                    arrival = Phase.Arrival.POISSON;

                // We now have the option to run all queries exactly once in a serial (rather than random) order.
                String serial_string;
                serial_string = work.getString("serial", "false");
                if (serial_string.equals("true")) {
                    serial = true;
                } else if (serial_string.equals("false")) {
                    serial = false;
                } else {
                    LOG.fatal("Serial string should be either 'true' or 'false'.");
                    System.exit(-1);
                }

                // We're not actually serial if we're running a script, so make
                // sure to suppress the serial flag in this case.
                serial = serial && (wrkld.getTraceReader() == null);

                int activeTerminals;
                activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
                activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);

                // If using serial, we should have only one terminal
                if (serial && activeTerminals != 1) {
                    LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
                    activeTerminals = 1;
                }

                if (activeTerminals > terminals) {
                    LOG.error(String.format("Configuration error in work %d: " +
                            "Number of active terminals is bigger than the total number of terminals", i));
                    System.exit(-1);
                }

                // Get the number of seconds for which the benchmark should run
                int time = work.getInt("/time", 0);
                timed = (time > 0);

                // Check if a script is to be run
                if (scriptRun) {
                    LOG.info("Running a script; ignoring timer, serial, and weight settings.");
                } else if (!timed) {
                    if (serial)
                        LOG.info("Timer disabled for serial run; will execute"
                                + " all queries exactly once.");
                    else {
                        LOG.fatal("Must provide positive time bound for"
                                + " non-serial executions. Either provide"
                                + " a valid time or enable serial mode.");
                        System.exit(-1);
                    }
                } else if (serial)
                    LOG.info("Timer enabled for serial run; will run queries"
                            + " serially in a loop until the timer expires.");

                wrkld.addWork(time,
                        setup.getTargetTPS(),
                        weight_strings,
                        rateLimited,
                        disabled,
                        serial,
                        timed,
                        activeTerminals,
                        arrival);
            }

            // -------------------------------------------------------------------
            //                       Validate Phases
            // -------------------------------------------------------------------

            int j = 0;
            for (Phase p : wrkld.getAllPhases()) {
                j++;
                if (p.getWeightCount() != wrkld.getNumTxnTypes()) {
                    LOG.fatal(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types",
                            j, p.getWeightCount(), wrkld.getNumTxnTypes()));
                    if (p.isSerial()) {
                        LOG.fatal("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                    }
                    System.exit(-1);
                }
            }

            // Generate the dialect map
            wrkld.init();

            assert (wrkld.getNumTxnTypes() >= 0);
            assert (xmlConfig != null);
        }

        assert (!benchList.isEmpty());
        assert (benchList.get(0) != null);
    }

    private static List<Results> runHybridWorkload(List<BenchmarkModule> benchList, int intervalMonitor,
                                                   double error_margin) throws IOException, InterruptedException {
        // ----------------------------------------
        //              Initialize Workers
        // ----------------------------------------

        BenchmarkModule bench = benchList.get(0);
        List<Worker> oltp_workers = new ArrayList<Worker>();
        List<Worker> olap_workers = new ArrayList<Worker>();

        WorkloadConfiguration workConf = bench.getWorkloadConfiguration();
        Clock clock = new Clock(workConf.getTargetTPS(), (int) workConf.getScaleFactor(), false, generateFilesPath);

        List<Results> results = new ArrayList<Results>();

        // ----------------------------------------
        //              Initialize Client Balancer
        // ----------------------------------------

        LOG.info("Creating CLIENT BALANCER");
        ClientBalancer clientBalancer = new ClientBalancer(bench, oltp_workers, olap_workers, bench.getWorkloadConfiguration(), clock, error_margin);
        Thread client_balancer = new Thread(clientBalancer);
        client_balancer.start();

        // ----------------------------------------
        //              OLTP Workers
        // ----------------------------------------

        oltp_workers.addAll(bench.makeWorkers("TPCC", clock));

        LOG.info("Created " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
        LOG.info("Started OLTP execution with " + bench.getWorkloadConfiguration().getTerminals() + " terminals.");
        LOG.info("Target TPS: " + bench.getWorkloadConfiguration().getTargetTPS() + " TPS");

        OLTPWorkerThread oltp_runnable = new OLTPWorkerThread(oltp_workers, workConf, intervalMonitor);
        Thread oltp = new Thread(oltp_runnable);
        oltp.start();

        // ----------------------------------------
        //              OLAP Workers
        // ----------------------------------------

        OLAPWorkerThread olap_runnable = new OLAPWorkerThread(olap_workers, workConf, intervalMonitor);
        Thread olap = new Thread(olap_runnable);
        olap.start();

        // ----------------------------------------
        //              Shutdown
        // ----------------------------------------

        oltp.join();
        olap.join();
        clientBalancer.terminate();
        client_balancer.join();

        // ----------------------------------------
        //      Wait until results are available
        // ----------------------------------------

        Results tpcc = oltp_runnable.getResults();
        Results tpch = olap_runnable.getResults();

        boolean proceed = false;
        while (!proceed) {
            if (tpcc != null && tpch != null) {
                proceed = true;
            } else {
                LOG.info("[HTAPB Thread]: Still waiting for results from OLTP and OLAP workers. Going to sleep for 1 minute...");
                Thread.sleep(60000);

                tpcc = oltp_runnable.getResults();
                tpch = olap_runnable.getResults();
            }
        }

        Results balancer = clientBalancer.getResults();

        results.add(tpcc);
        results.add(tpch);
        results.add(balancer);

        return results;
    }

    private static List<Results> runOLTPWorkload(List<BenchmarkModule> benchList, int intervalMonitor)
            throws IOException, InterruptedException {
        // ----------------------------------------
        //              Initialize Workers
        // ----------------------------------------

        BenchmarkModule bench = benchList.get(0);
        List<Worker> oltp_workers = new ArrayList<Worker>();

        WorkloadConfiguration workConf = bench.getWorkloadConfiguration();
        Clock clock = new Clock(workConf.getTargetTPS(), (int) workConf.getScaleFactor(), false, generateFilesPath);

        List<Results> results = new ArrayList<Results>();

        // ----------------------------------------
        //              OLTP Workers
        // ----------------------------------------

        oltp_workers.addAll(bench.makeWorkers("TPCC", clock));

        LOG.info("Created " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
        LOG.info("Started OLTP execution with " + bench.getWorkloadConfiguration().getTerminals() + " terminals.");
        LOG.info("Target TPS: " + bench.getWorkloadConfiguration().getTargetTPS() + " TPS");

        OLTPWorkerThread oltp_runnable = new OLTPWorkerThread(oltp_workers, workConf, intervalMonitor);
        Thread oltp = new Thread(oltp_runnable);
        oltp.start();

        // ----------------------------------------
        //              Shutdown
        // ----------------------------------------

        oltp.join();

        // ----------------------------------------
        //      Wait until results are available
        // ----------------------------------------

        Results tpcc = oltp_runnable.getResults();

        boolean proceed = false;
        while (!proceed) {
            if (tpcc != null) {
                proceed = true;
            } else {
                LOG.info("[HTAPB Thread]: Still waiting for results from OLTP workers. Going to sleep for 1 minute...");
                Thread.sleep(60000);

                tpcc = oltp_runnable.getResults();
            }
        }

        results.add(tpcc);

        return results;
    }

    private static List<Results> runOLAPWorkload(List<BenchmarkModule> benchList, int intervalMonitor)
            throws IOException, InterruptedException {
        // ----------------------------------------
        //              Initialize Workers
        // ----------------------------------------

        BenchmarkModule bench = benchList.get(0);
        List<Worker> olap_workers = new ArrayList<Worker>();

        WorkloadConfiguration workConf = bench.getWorkloadConfiguration();
        Clock clock = new Clock(workConf.getTargetTPS(), (int) workConf.getScaleFactor(), false, generateFilesPath);

        List<Results> results = new ArrayList<Results>();

        // ----------------------------------------
        //              OLAP Workers
        // ----------------------------------------

        olap_workers.addAll(bench.makeOLAPWorker(clock));

        LOG.info("Created " + bench.getWorkloadConfiguration().getTerminals() + " virtual terminals...");
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                bench.getBenchmarkName(), bench.getWorkloadConfiguration().getNumberOfPhases()));
        LOG.info("Started OLAP execution with " + bench.getWorkloadConfiguration().getTerminals() + " terminals.");

        OLAPWorkerThread olap_runnable = new OLAPWorkerThread(olap_workers, workConf, intervalMonitor);
        Thread olap = new Thread(olap_runnable);
        olap.start();

        // ----------------------------------------
        //              Shutdown
        // ----------------------------------------

        olap.join();

        // ----------------------------------------
        //      Wait until results are available
        // ----------------------------------------

        Results tpch = olap_runnable.getResults();

        boolean proceed = false;
        while (!proceed) {

            if (tpch != null) {
                proceed = true;
            } else {
                LOG.info("[HTAPB Thread]: Still waiting for results from OLAP workers. Going to sleep for 1 minute...");
                Thread.sleep(60000);

                tpch = olap_runnable.getResults();
            }
        }

        results.add(tpch);

        return results;
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to true.
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null && val.equalsIgnoreCase("true"));
        }
        return (false);
    }

}
