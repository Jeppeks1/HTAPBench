
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
import java.sql.Timestamp;
import java.util.*;

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

import pt.haslab.htapbench.benchmark.*;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.api.TransactionTypes;
import pt.haslab.htapbench.configuration.Configuration;
import pt.haslab.htapbench.configuration.Configuration.Mode;
import pt.haslab.htapbench.configuration.workload.Analytical;
import pt.haslab.htapbench.configuration.workload.Hybrid;
import pt.haslab.htapbench.configuration.workload.Transactional;
import pt.haslab.htapbench.types.DatabaseType;
import pt.haslab.htapbench.util.FileUtil;
import pt.haslab.htapbench.util.ResultUploader;
import pt.haslab.htapbench.util.StringUtil;

import static pt.haslab.htapbench.benchmark.HTAPBConstants.dateFormat;

public class HTAPBench {

    private static final Logger LOG = Logger.getLogger(HTAPBench.class);
    private static final String SINGLE_LINE = "**********************************************************************************";

    // This list is used for filtering of the output in case there are inactive transaction types
    private static List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();

    // A list of BenchmarkModules is used to possibly run a benchmark test with more than one Phase
    private static List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();

    // The Workload to be run
    private static Workload workload;

    // Command line arguments that are needed in the WorkloadConfiguration
    private static double error_margin;
    private static boolean calibrate;
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
        options.addOption("m", "mode", true, "[required] Mode indicating the benchmark configuration strategy." +
                " Supported options: " + Arrays.toString(Mode.values()));
        options.addOption(null, "workload", true, "Which workload to run, default hybrid.");
        options.addOption(null, "sequence", true, "Performs every mode in sequence before the current mode, default true.");
        options.addOption(null, "overwrite", false, "Resets the database or CSV files in either the configure or generate mode.");
        options.addOption(null, "useCSV", true, "Use CSV files in the generate or populate phase, default true.");
        options.addOption(null, "filePathCSV", true, "Path to the CSV file directory for populating the database, default is ../csv/?tps.");
        options.addOption(null, "runscript", true, "Run an SQL script");
        options.addOption(null, "upload", true, "Upload the result");
        options.addOption("h", "help", false, "Print this help");
        options.addOption("s", "sample", true, "Aggregate the results every 'sample' seconds, default 60 seconds.");
        options.addOption("ss", false, "Verbose Sampling per Transaction");
        options.addOption("d", "directory", true, "Base directory for the result files, default is ./results/?tps");
        options.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
        options.addOption("ts", "tracescript", true, "Script of transactions to execute");
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
        } else if (!argsLine.hasOption("m")) {
            LOG.fatal("Missing Benchmark configuration mode");
            printUsage(options);
            return;
        }

        // Retrieve the requested Configuration configuration mode
        String inputMode = argsLine.getOptionValue("mode").toLowerCase();
        Mode mode = validateMode(inputMode);
        if (mode == Mode.INVALID) {
            LOG.fatal("Invalid mode " + inputMode + ". Print the help to see supported modes.");
            return;
        }

        // Get the configuration file
        String configFile = argsLine.getOptionValue("c");
        XMLConfiguration xmlConfig = new XMLConfiguration(configFile);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());

        // Store the output directory of the results if given, otherwise use the default "results" directory
        String outputDirectory = "results/" + (int) xmlConfig.getDouble("target_TPS") + "tps";
        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }

        // The timestamp value at the beginning of the test is appended to result files
        String timestampValue = "";
        if (argsLine.hasOption("t")) {
            timestampValue = dateFormat.format(new Timestamp(System.currentTimeMillis())) + "_";
        }

        // Check if the idealClient flag has been set, indicating how the workload setup should proceed
        idealClient = argsLine.hasOption("ic");

        // Check if the command line contained overwrite parameter
        boolean overwrite = argsLine.hasOption("overwrite");

        // Check if the command line contained the sequence parameter
        boolean sequence = isBooleanOptionSet(argsLine, "sequence");

        // Get the standalone script if given
        String script = argsLine.getOptionValue("runscript");

        // Retrieve the sampling window parameter or set the default value
        int windowSize = Integer.parseInt(argsLine.getOptionValue("sample", "60"));

        // Retrieve the calibrate variable
        calibrate = isBooleanOptionSet(argsLine, "calibrate");

        // Get the error margin allowed for hybrid workloads
        error_margin = xmlConfig.getDouble("error_margin");

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

        // Initialize the Configuration to prepare for the benchmark
        for (BenchmarkModule benchmark : benchList) {
            // Instantiate the appropriate Configuration class and configure the configuration
            Configuration configuration = Configuration.instantiate(benchmark);
            configuration.prepareDatabase(mode, overwrite, sequence);

            // Invoke the standalone script if given
            if (isBooleanOptionSet(argsLine, "runscript"))
                configuration.runScript(script);
        }

        // Execute Workload
        if (mode == Mode.EXECUTE) {
            // Bombs away!
            List<Results> results = workload.execute();

            // -------------------------------------------------------------------
            //                     Collect and print results
            // -------------------------------------------------------------------

            assert (results != null);

            PrintStream ps = System.out;
            PrintStream rs = System.out;

            // Special result uploader
            for (Results r : results) {

                // Null results occurs when the workload is not hybrid - skip them
                if (r == null)
                    continue;

                LOG.info(r.getClass());
                ResultUploader ru = new ResultUploader(r, xmlConfig, windowSize);

                // Make the output directory if it does not already exist
                FileUtil.makeDirIfNotExists(outputDirectory);

                // Build the complex path with the requested filename and possibly with the timestamp prepended
                String baseFile = timestampValue + r.getName().toLowerCase();

                // Increment the filename for new results
                String nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));
                ps = new PrintStream(new File(nextName));
                LOG.info("Output into file: " + nextName);

                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".raw"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output Raw data into file: " + nextName);

                if (windowSize != 0) {
                    LOG.info("Grouped into Buckets of " + windowSize + " seconds");
                    r.writeCSV(windowSize, ps);

                    if (isBooleanOptionSet(argsLine, "upload")) {
                        ru.uploadResult();
                    }

                    // Allow more detailed reporting by transaction to make it easier to check
                    if (argsLine.hasOption("ss")) {
                        for (TransactionType t : activeTXTypes) {
                            PrintStream ts = ps;

                            // Do not write TPCC results for analytical queries
                            if (r.getName().equalsIgnoreCase("TPCC") && t.getName().contains("Q"))
                                continue;

                            // Do not write TPCH results for transactional queries
                            if (r.getName().equalsIgnoreCase("TPCH") && !t.getName().contains("Q"))
                                continue;

                            // ClientBalancer results can not be more detailed
                            if (r.getName().equalsIgnoreCase("ClientBalancer"))
                                continue;

                            if (ts != System.out) {
                                // Get the actual filename for the output
                                String file = baseFile + "_" + t.getName();
                                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, file + ".res"));
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

            // The results from the TPCC and TPCH workloads already contain just a single histogram
            Results singleHistogram = workload instanceof Transactional ? results.get(0) : results.get(1);

            // Combine the histograms into a single histogram in case of a hybrid workload
            if (workload instanceof Hybrid)
                singleHistogram = results.get(0).combineHistograms(results.get(1));

            // Write the histograms to the log
            LOG.info(SINGLE_LINE);
            if (!singleHistogram.getSuccessHistogram().isEmpty())
                LOG.info("Completed Transactions:\n" + singleHistogram.getSuccessHistogram() + "\n");

            if (!singleHistogram.getAbortedHistogram().isEmpty())
                LOG.info("Aborted Transactions:\n" + singleHistogram.getAbortedHistogram() + "\n");

            if (!singleHistogram.getRetryHistogram().isEmpty())
                LOG.info("Retried Transactions:\n" + singleHistogram.getRetryHistogram() + "\n");

            if (!singleHistogram.getErrorHistogram().isEmpty())
                LOG.info("Unexpected Errors:\n" + singleHistogram.getErrorHistogram() + "\n");

            if (!singleHistogram.getRecordedMessagesHistogram().isEmpty())
                LOG.info("Recorded exceptions:\n" + StringUtil.formatRecordedMessages(singleHistogram.getRecordedMessagesHistogram()));

            ps.close();
            rs.close();
        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }

    private static Mode validateMode(String inputMode) {
        switch (inputMode) {
            case "configure":
                return Mode.CONFIGURE;
            case "generate":
                return Mode.GENERATE;
            case "populate":
                return Mode.POPULATE;
            case "execute":
                return Mode.EXECUTE;
            default:
                return Mode.INVALID;
        }
    }

    private static Workload initializeWorkload(String workloadString, HTAPBenchmark bench) {
        Workload workload;
        switch (workloadString) {
            case "oltp":
            case "tpcc":
                workload = new Transactional(bench);
                break;
            case "olap":
            case "tpch":
                workload = new Analytical(bench);
                break;
            case "htap":
            case "hybrid":
                workload = new Hybrid(bench);
                break;
            default:
                throw new RuntimeException("Workload " + workloadString + " not recognized as a valid option.");
        }

        // Return the Workload instance
        return workload;
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

    private static void configureBenchmarks(WorkloadSetup setup, CommandLine argsLine, String configFile,
                                            XMLConfiguration xmlConfig, XMLConfiguration pluginConfig)
            throws ParseException {
        // Retrieve the filePath if the value is set, otherwise use the default data location "../csv/?tps"
        String filePathCSV = "../csv/" + (int) xmlConfig.getDouble("target_TPS") + "tps";
        if (argsLine.hasOption("filePathCSV")) {
            filePathCSV = argsLine.getOptionValue("filePathCSV");
        }

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
            wrkld.setRecordAbortMessages(xmlConfig.getBoolean("recordabortmessages", true));
            wrkld.setErrorMargin(error_margin);
            wrkld.setIdealClient(idealClient);
            wrkld.setScaleFactor(setup.getWarehouses());
            wrkld.setTerminals(setup.getTerminals());
            wrkld.setTargetTPS(setup.getTargetTPS());
            wrkld.setFilePathCSV(filePathCSV);

            // Simulate error in original implementation where calibrate was hardcoded to true
            wrkld.setCalibrate(Mode.CONFIGURE);

            // Set the useCSV value according to the input
            if (argsLine.hasOption("useCSV")) {
                wrkld.setUseCSV(Boolean.parseBoolean(argsLine.getOptionValue("useCSV")));
            }

            // Configure based on the requested workload
            if (isBooleanOptionSet(argsLine, "oltp")) {
                wrkld.setScaleFactor(xmlConfig.getDouble("warehouses"));
                terminals = (int) xmlConfig.getDouble("warehouses") * 10;
                wrkld.setTerminals(terminals);
                wrkld.setHybridWorkload(false);
                rateLimited = false;
            } else if (isBooleanOptionSet(argsLine, "olap")) {
                setup.setWarehouses(xmlConfig.getInt("warehouses"));
                wrkld.setScaleFactor(xmlConfig.getDouble("warehouses"));
                terminals = (int) xmlConfig.getDouble("OLAP_workers");
                wrkld.setTerminals(terminals);
                wrkld.setHybridWorkload(false);
                rateLimited = false;
            } else {
                // HTAP workload
                wrkld.setTerminals(setup.getTerminals());
                terminals = setup.getTerminals();
                wrkld.setScaleFactor(setup.getWarehouses());
                wrkld.setTargetTPS(setup.getTargetTPS());
                wrkld.setHybridWorkload(true);
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

            // Create the benchmark for this workload configuration
            HTAPBenchmark bench = new HTAPBenchmark(wrkld);

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

            benchList.add(bench);

            // Initialize the Workload
            String workloadName = argsLine.getOptionValue("workload", "hybrid");
            workload = initializeWorkload(workloadName, bench);

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
