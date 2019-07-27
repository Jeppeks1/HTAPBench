package pt.haslab.htapbench.configuration.workload;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.core.Workload;
import pt.haslab.htapbench.core.*;

import java.util.ArrayList;
import java.util.List;

public class Hybrid extends Workload {

    private static final Logger LOG = Logger.getLogger(Analytical.class);
    private static final int fixedOlapCount = 22;

    public Hybrid(HTAPBenchmark benchmark) {
        super(benchmark);
    }

    // -------------------------------------------------------------------
    //                        Implementation
    // -------------------------------------------------------------------

    public void initializeWorkers() {

        // ----------------------------------------
        //              OLTP Workers
        // ----------------------------------------

        workers.addAll(bench.makeWorkers("TPCC"));

        LOG.info("Created " + bench.getWorkloadConfiguration().getTerminals() + " virtual TPCC terminals...");
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                bench.getBenchmarkName(), phases.size()));
        LOG.info("Started OLTP execution with " + bench.getWorkloadConfiguration().getTerminals() + " terminals.");
        LOG.info("Target TPS: " + bench.getWorkloadConfiguration().getTargetTPS() + " TPS");

        // ----------------------------------------
        //              OLAP Workers
        // ----------------------------------------

        // Check if a special strategy is required when selecting which OLAP query to run
        if (bench.getWorkloadConfiguration().getHybridStrategy().equalsIgnoreCase("fixed")){
            // Add a fixed amount of OLAP workers
            List<Worker> tempList = new ArrayList<Worker>(fixedOlapCount);
            for (int i = 0; i < fixedOlapCount; i++) {
                tempList.add(bench.makeOLAPWorker());
            }
            workers.addAll(tempList);
        }

        // If the above option is not set, the ClientBalancer spawns new OLAP workers every minute.
    }

}
