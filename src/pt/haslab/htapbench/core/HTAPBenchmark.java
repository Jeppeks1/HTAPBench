/*
 * Copyright 2017 by INESC TEC
 * Developed by Fábio Coelho
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import pt.haslab.htapbench.benchmark.BenchmarkModule;
import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;
import pt.haslab.htapbench.procedures.tpcc.NewOrder;
import pt.haslab.htapbench.procedures.tpch.Q1;

import java.util.concurrent.atomic.AtomicInteger;

public class HTAPBenchmark extends BenchmarkModule {

    private static final Logger LOG = Logger.getLogger(HTAPBenchmark.class);
    private AtomicInteger ts_counter;
    private Clock clock;

    public HTAPBenchmark(WorkloadConfiguration workConf) {
        super(workConf.getBenchmarkName(), workConf, true);

        this.ts_counter = new AtomicInteger();
    }

    /**
     * Initialize the clock for the execution phase
     */
    void initClock() {
        this.clock = new Clock(workConf.getHybridWorkload(), workConf.getFilePathCSV());
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (NewOrder.class.getPackage());
    }

    /**
     * Imports packages for OLTP and OLAP queries.
     * This is necessary once they live in different packages for clarity.
     */
    protected Package getProcedurePackageImpl(String txnName) {
        if (txnName.startsWith("Q"))
            return (Q1.class.getPackage());
        else
            return (NewOrder.class.getPackage());
    }

    /**
     * This method either creates terminals for the OLTP or the OLAP stream.
     */
    @Override
    protected List<Worker> makeWorkersImpl(String workerType) {
        ArrayList<Worker> workers = new ArrayList<Worker>();

        if (workerType.equals("TPCC")) {
            try {
                List<TPCCWorker> terminals = createTerminalsOLTP();
                workers.addAll(terminals);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (workerType.equals("TPCH")) {
            try {
                List<TPCHWorker> terminals = createTerminalsOLAP();
                workers.addAll(terminals);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return workers;
    }


    /**
     * Returns one OLAP terminal.
     */
    @Override
    protected TPCHWorker makeOneOLAPWorkerImpl() {
        return new TPCHWorker(this, clock);
    }

    /**
     * This methods creates OLTP terminals.
     */
    private ArrayList<TPCCWorker> createTerminalsOLTP() {

        TPCCWorker[] terminals = new TPCCWorker[workConf.getTerminals()];

        int numWarehouses = (int) workConf.getScaleFactor();
        int numTerminals = workConf.getTerminals();

        assert (numTerminals >= numWarehouses) :
                String.format("Insufficient number of terminals '%d' [numWarehouses=%d]",
                        numTerminals, numWarehouses);

        int warehouseOffset = Integer.getInteger("warehouseOffset", 1);
        assert warehouseOffset == 1;

        // We distribute terminals evenly across the warehouses
        // Eg. if there are 10 terminals across 7 warehouses, they
        // are distributed as 1, 1, 2, 1, 2, 1, 2
        final double terminalsPerWarehouse = (double) numTerminals / numWarehouses;
        assert terminalsPerWarehouse >= 1;

        for (int w = 0; w < numWarehouses; w++) {
            // Compute the number of terminals in *this* warehouse
            int lowerTerminalId = (int) (w * terminalsPerWarehouse);
            int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);

            // Protect against double rounding errors
            int w_id = w + 1;
            if (w_id == numWarehouses)
                upperTerminalId = numTerminals;
            int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

            LOG.info(String.format("w_id %d = %d terminals [lower=%d / upper%d]",
                    w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));

            final double districtsPerTerminal = HTAPBConstants.configDistPerWhse / (double) numWarehouseTerminals;
            assert districtsPerTerminal >= 1 :
                    String.format("Too many terminals [districtsPerTerminal=%.2f, numWarehouseTerminals=%d]",
                            districtsPerTerminal, numWarehouseTerminals);

            for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
                int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
                int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
                if (terminalId + 1 == numWarehouseTerminals) {
                    upperDistrictId = HTAPBConstants.configDistPerWhse;
                }

                lowerDistrictId += 1;

                TPCCWorker terminal = new TPCCWorker(w_id, lowerDistrictId, upperDistrictId,
                                                     numWarehouses, this, ts_counter, clock);

                terminals[lowerTerminalId + terminalId] = terminal;
            }

        }
        assert terminals[terminals.length - 1] != null;

        ArrayList<TPCCWorker> ret = new ArrayList<TPCCWorker>();
        ret.addAll(Arrays.asList(terminals));

        return ret;
    }

    /**
     * This method creates OLAP terminals.
     */
    private ArrayList<TPCHWorker> createTerminalsOLAP() {
        int numTerminals = workConf.getTerminals();

        ArrayList<TPCHWorker> ret = new ArrayList<TPCHWorker>();
        LOG.info(String.format("Creating %d workers for TPC-H", numTerminals));
        for (int i = 0; i < numTerminals; i++)
            ret.add(new TPCHWorker(this, clock));

        return ret;
    }

    public Clock getClock(){
        return clock;
    }
}
