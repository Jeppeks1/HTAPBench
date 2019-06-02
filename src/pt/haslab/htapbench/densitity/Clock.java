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
    private long deltaTs;
    private int warehouses;
    private long startTime;
    private long populateStartTs;

    public Clock(long deltaTS, boolean populate, String filePath) {
        this.deltaTs = deltaTS;
        this.warehouses = 0;

        if (populate) {
            this.startTime = System.currentTimeMillis();
            this.populateStartTs = getFinalPopulatedTs();
        } else {
            this.startTime = AuxiliarFileHandler.importLastTs(filePath);
            this.populateStartTs = AuxiliarFileHandler.importFirstTs(filePath);
        }

        this.clock = new AtomicLong(startTime);
    }

    public Clock(long deltaTS, int warehouses, boolean populate, String filePath) {
        this.deltaTs = deltaTS;
        this.warehouses = warehouses;

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
     * Computes the Timestamp to be used during dynamic query generation.
     *
     * @param ts : A target Timestamp
     * @return A new timestamp which is the offset between ts and the start Ts
     * in the TPC-H spec transformed to our dataset.
     */
    public long transformTsFromSpecToLong(long ts) {
        //number of timestamps;
        long tss = this.startTime - this.populateStartTs;
        //number of ts in each slot;
        int tpch_populate_slots = 2555;
        long step = tss / tpch_populate_slots;

        int diff_days = (int) ((ts - tpch_start_date) / (24 * 60 * 60 * 1000));

        return this.populateStartTs + diff_days * step;
    }

    /**
     * Computes Timestamp - days.
     */
    public long computeTsMinusXDays(long ts, int days) {
        long daysInLong = days * 24 * 60 * 60 * 1000;
        return ts - daysInLong;
    }

}
