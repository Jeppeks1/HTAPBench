package pt.haslab.htapbench.configuration.workload;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.core.Workload;
import pt.haslab.htapbench.core.*;

public class Hybrid extends Workload {

    private static final Logger LOG = Logger.getLogger(Analytical.class);

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

        // The ClientBalancer spawns new OLAP workers every minute.
    }

}
