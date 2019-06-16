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
package pt.haslab.htapbench.densitity;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

import pt.haslab.htapbench.benchmark.jTPCCConfig;
import pt.haslab.htapbench.core.AuxiliarFileHandler;

/**
 * This class sets up the Clock considering the deltaTs found in density class.
 * The main purpose is to increment a global clock according to the deltaTs.
 * This will allow to reproduce in load time, the TS density found during TPC-C
 * execution for a given target TPS.
 */
public class Clock {

    private final long tpch_start_date = Timestamp.valueOf("1992-01-01 00:00:00").getTime();
    private final long tpch_end_date = Timestamp.valueOf("1998-12-31 23:59:59").getTime();
    private AtomicLong clock;
    private boolean populate;
    private boolean hybrid;
    private long populateStartTs;
    private long deltaTs;
    private long startTime;
    private int warehouses;

    /**
     * Clock constructor for the population phase.
     */
    public Clock(long deltaTS, int warehouses, boolean populate, String filePath) {
        this.deltaTs = deltaTS;
        this.warehouses = warehouses;
        this.populate = populate;

        if (populate) {
            this.startTime = System.currentTimeMillis();
            this.populateStartTs = getFinalPopulatedTs();
        } else {
            this.startTime = AuxiliarFileHandler.importLastTs(filePath);
            this.populateStartTs = AuxiliarFileHandler.importFirstTs(filePath);
        }

        this.clock = new AtomicLong(startTime);
    }

    /**
     * Clock constructor for the execution phase.
     */
    public Clock(long deltaTS, int warehouses, boolean populate, boolean hybrid, String filePath) {
        this.deltaTs = deltaTS;
        this.warehouses = warehouses;
        this.populate = populate;
        this.hybrid = hybrid;

        if (populate) {
            this.startTime = System.currentTimeMillis();
            this.populateStartTs = startTime;
        } else {
            this.startTime = AuxiliarFileHandler.importLastTs(filePath);
            this.populateStartTs = AuxiliarFileHandler.importFirstTs(filePath);
        }

        this.clock = new AtomicLong(startTime);
    }

    /**
     * Increments the global clock and returns the new timestamp.
     */
    public long tick() {
        return clock.addAndGet(deltaTs);
    }

    /**
     * Returns the current value of the global clock.
     */
    public long getCurrentTs() {
        return clock.get();
    }

    /**
     * Returns the first generated Timestamp.
     */
    public long getStartTimestamp() {
        return this.startTime;
    }

    /**
     * Computes and returns the last populated TS.
     */
    public long getFinalPopulatedTs() {
        long TSnumber = warehouses * jTPCCConfig.configDistPerWhse * jTPCCConfig.configCustPerDist * 2;
        return startTime + deltaTs * TSnumber;
    }

    /**
     * Computes and returns the offset of the sliding window
     * to be used in case of a hybrid workload.
     */
    private long getSlidingWindowOffset() {
        // Check if we are in the population phase and if a sliding window is necessary
        if (populate || !hybrid)
            return 0;
        else
            return getCurrentTs() - getStartTimestamp();
    }


    /**
     * Computes the Timestamp to be used during dynamic query generation to ensure
     * comparable complexity across runs.
     *
     * The timestamps in the population phase are generated using a fixed amount
     * of time between each timestamp (a 'tick') and covers a relatively short
     * period of time, while the TPC-H specification covers a 7 year period.
     *
     * This method transforms the randomly generated TPC-H timestamps to the equivalent
     * timestamp in the population phase using the Clock. A 'sliding window' with a
     * fixed size equal to the timestamp interval in the population phase is used
     * in case of a hybrid workload, to also consider newly created data by the TPC-C
     * workload.
     *
     * @param ts A TPC-H timestamp
     * @return A new timestamp consistent with the timestamps in the population phase
     *         to be used as date parameters in TPC-H queries.
     */
    public long transformTsFromSpecToLong(long ts) {
        // Determine the size of the interval in the population phase
        long tss = this.startTime - this.populateStartTs;

        // Determine how far into the TPCH time interval the timestamp is, as a percentage
        double pct = (double) (ts - tpch_start_date)/(tpch_end_date - tpch_start_date);

        // Determine the offset, possibly including a sliding window
        long offset = (long) (getSlidingWindowOffset() + pct * tss);

        // Return the offset with populateStartTs as the reference
        return offset + this.populateStartTs;
    }

    /**
     * Computes Timestamp - days.
     */
    public long computeTsMinusXDays(long ts, int days) {
        long daysInLong = days * 24 * 60 * 60 * 1000;
        return ts - daysInLong;
    }

}
