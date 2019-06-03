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

    private final double m = 1.26956;
    private final double b = 0.0103497;
    private double density;
    private long deltaTS; //deltaTs in ms.
    private final int targetTPS;


    DensityConsultant(int targetTPS) {
        this.targetTPS = targetTPS;
        this.computeDensity();
        this.computeDeltaTS();
    }

    /**
     * Computes the density found during the TPC-C execute stage. This value is pre-defines for the standard TPC-X Txn mix.
     */
    private void computeDensity() {
        this.density = m * targetTPS + b;
    }

    /**
     * Return how many seconds should the clock move forward at each new TS issue process.
     */
    private void computeDeltaTS() {
        this.deltaTS = (long) (1000 / density);
    }

    /**
     * Computes the Timestamp Delta used at each clock tick.
     */
    public long getDeltaTs() {
        return this.deltaTS;
    }
}