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

/**
 * This class houses the density specification for a standard mix execution of TPC-C with think time.
 */
public class DensityConsultant {

    // The timestamps/second found in the population phase.
    private double populateOrderTPS = Clock.targetPopulateTsPerSec;
    private double populateDeliveryTPS = populateOrderTPS * 0.7; // 70 % of all o_entry_d are re-used as ol_delivery_d

    // The amount the Clock should be incremented, to match the timestamps/second found in the population phase.
    private long newOrderDeltaTs;
    private long deliveryDeltaTs;

    DensityConsultant() {
        computeDeltaTS();
    }

    /**
     * Return how many milliseconds the clock should move forward at each new TS issue process.
     */
    private void computeDeltaTS() {
        // Compute the deltaTS for the execution phase based on the number of timestamps
        // per second in the population phase.
        this.newOrderDeltaTs = (long) Math.floor(1000 / populateOrderTPS);
        this.deliveryDeltaTs = (long) Math.floor(1000 / populateDeliveryTPS);
    }

    /**
     * Computes the Timestamp delta used at each clock tick for the newOrder transaction.
     */
    long getNewOrderDeltaTs() {
        return newOrderDeltaTs;
    }

    /**
     * Computes the Timestamp delta used at each clock tick for the delivery transaction.
     */
    long getDeliveryDeltaTs() {
        return deliveryDeltaTs;
    }
}
