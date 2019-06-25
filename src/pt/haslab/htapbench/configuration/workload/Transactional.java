package pt.haslab.htapbench.configuration.workload;

import org.apache.log4j.Logger;
import pt.haslab.htapbench.core.HTAPBenchmark;
import pt.haslab.htapbench.core.Workload;

public class Transactional extends Workload {

    private static final Logger LOG = Logger.getLogger(Transactional.class);

    public Transactional(HTAPBenchmark bench) {
        super(bench);
    }

    // -------------------------------------------------------------------
    //                        Implementation
    // -------------------------------------------------------------------

    @Override
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
    }
}
