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

import java.sql.Timestamp;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.benchmark.AuxiliarFileHandler;
import pt.haslab.htapbench.util.FileUtil;

/**
 * This class sets up the Clock considering the deltaTs found in density class.
 * The main purpose is to increment a global clock according to the deltaTs.
 * This will allow to reproduce in load time, the TS density found during TPC-C
 * execution for a given target TPS.
 */
public class Clock {

    // The target number of timestamps/second in the population phase. This particular
    // value of 25 reduces the rounding error when converting to a long.
    static int targetPopulateTsPerSec = 25;

    private final long tpch_start_date = Timestamp.valueOf("1992-01-01 00:00:00").getTime();
    private final long tpch_end_date = Timestamp.valueOf("1998-12-31 23:59:59").getTime();
    private DensityConsultant density;
    private AtomicLong newOrderClock;
    private AtomicLong deliveryClock;
    private int intervalSizeSeconds;
    private long startTime;
    private long populateStartTs;
    private boolean hybrid;

    /**
     * Clock constructor for the population phase.
     */
    public Clock(int warehouses, String filePath) {
        int timestamps = warehouses * HTAPBConstants.configDistPerWhse * HTAPBConstants.configCustPerDist;
        this.intervalSizeSeconds = timestamps / targetPopulateTsPerSec;

        FileUtil.makeDirIfNotExists(filePath);

        this.startTime = System.currentTimeMillis();
        this.populateStartTs = startTime;
    }

    /**
     * Clock constructor for the execution phase.
     */
    public Clock(boolean hybrid, String filePath) {
        this.startTime = AuxiliarFileHandler.importLastTs(filePath);
        this.populateStartTs = AuxiliarFileHandler.importFirstTs(filePath);

        this.hybrid = hybrid;

        // Set the start-time for both clocks
        this.newOrderClock = new AtomicLong(startTime);
        this.deliveryClock = new AtomicLong(startTime);

        this.density = new DensityConsultant();
    }

    /**
     * Increments the newOrder clock and returns the new timestamp.
     */
    public long newOrdertick() {
        return newOrderClock.addAndGet(density.getNewOrderDeltaTs());
    }

    /**
     * Increments the delivery clock and returns the new timestamp.
     */
    public long deliveryTick() {
        return deliveryClock.addAndGet(density.getDeliveryDeltaTs());
    }

    /**
     * Generates a random long between the start and finish timestamp of the population
     * phase. The size of the interval between the two timestamps are already defined
     * with respect to deltaTs, so the tick() method in the population phase does not
     * lead to a sudden promotion in timestamps.
     *
     * @return a random long between the start and finish timestamp of the population phase.
     */
    public long populateTick() {
        return ThreadLocalRandom.current().nextLong(startTime, getFinalPopulatedTs());
    }

    /**
     * Returns the first generated Timestamp.
     */
    public long getStartTimestamp() {
        return startTime;
    }

    /**
     * Computes and returns the last populated TS.
     */
    public long getFinalPopulatedTs() {
        return startTime + intervalSizeSeconds * 1000;
    }

    public long getCurrentTs() {
        return Math.max(newOrderClock.get(), deliveryClock.get());
    }

    /**
     * Computes and returns the offset of the sliding window to be used
     * in case of a hybrid workload.
     */
    private long getSlidingWindowOffset(AtomicLong clock) {
        // Check if we are in the population phase and if a sliding window is necessary
        if (!hybrid)
            return 0;
        else
            return clock.get() - getStartTimestamp();
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
    private long transformTsFromSpecToLong(AtomicLong clock, long ts) {
        // Determine the size of the interval in the population phase
        long tss = this.startTime - this.populateStartTs;

        // Determine how far into the TPCH time interval the timestamp is, as a percentage
        double pct = (double) (ts - tpch_start_date)/(tpch_end_date - tpch_start_date);

        // Determine the offset, possibly including a sliding window
        long offset = (long) (getSlidingWindowOffset(clock) + pct * tss);

        // Return the offset with populateStartTs as the reference
        return offset + this.populateStartTs;
    }

    public long transformOrderTsToLong(long ts) {
        return transformTsFromSpecToLong(newOrderClock, ts);
    }

    public long transformDeliveryTsToLong(long ts) {
        return transformTsFromSpecToLong(deliveryClock, ts);
    }

    /**
     * Offsets the TPC-H enddate with ´days` days.
     *
     * The enddate can be used in this context, because the specification
     * of Q1 states that the largest possible delivery date should be used.
     * TPC-H actually uses enddate - 30 days as the maximum delivery date,
     * but this distinction is not included in the Loader class, meaning
     * the very last timestamp can be used to set the delivery date.
     *
     * @param days timestamp offset in days
     * @return the TPCH timestamp offset with ´days´ days
     */
    public long computeEndMinusXDays(int days) {
        long daysInLong = days * 24 * 60 * 60 * 1000L;
        return tpch_end_date - daysInLong;
    }

    public long computeTsPlusXTransformedDays(long ts, int days) {
        // Determine the size of the interval in the population phase
        long tss = getFinalPopulatedTs() - getStartTimestamp();

        // Transform the number of days into the 7-year interval used in TPCH
        double pct = (double) days / (365 * 7);

        // Determine the offset
        long offset = (long) (pct * tss);

        // Return the new timestamp
        return ts + offset;
    }

    public double computeTransformScaleFactor() {
        // Determine the size of the interval in the population phase
        long tss = getFinalPopulatedTs() - getStartTimestamp();

        // Multiplying this value with the number of days returns the size of the offset
        // See computeTsPlusXTransformedDays for a clearer interpretation of why this works.
        return (double) tss / (365 * 7);
    }
}
