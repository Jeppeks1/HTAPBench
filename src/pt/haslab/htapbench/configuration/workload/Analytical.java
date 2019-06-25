package pt.haslab.htapbench.configuration.workload;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.core.HTAPBenchmark;
import pt.haslab.htapbench.core.Workload;

public class Analytical extends Workload {

    private static final Logger LOG = Logger.getLogger(Analytical.class);

    public Analytical(HTAPBenchmark bench) {
        super(bench);
    }

    // -------------------------------------------------------------------
    //                        Implementation
    // -------------------------------------------------------------------

    public void initializeWorkers() {

        // ----------------------------------------
        //              OLAP Workers
        // ----------------------------------------

        workers.addAll(bench.makeWorkers("TPCH"));

        LOG.info("Created " + bench.getWorkloadConfiguration().getTerminals() + " virtual TPCH terminals...");
        LOG.info(String.format("Launching the %s Benchmark with %s Phases...",
                bench.getBenchmarkName(), phases.size()));
        LOG.info("Started OLAP execution with " + bench.getWorkloadConfiguration().getTerminals() + " terminals.");
    }

}
