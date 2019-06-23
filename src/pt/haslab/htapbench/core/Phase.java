
/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************
/*
 * Copyright 2017 by INESC TEC                                                                                                
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
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Phase {
    public enum Arrival {
        REGULAR, POISSON,
    }

    private final Random gen = new Random();
    private final String benchmarkName;
    public final int id;
    public int time;
    final int rate;
    final Arrival arrival;

    private final boolean rateLimited;
    private final boolean disabled;
    private final boolean serial;
    private final boolean timed;
    private final List<Double> weights;
    private final int num_weights;
    private int activeTerminals;
    private int nextSerial;

    public Phase(String benchmarkName, int id, int t, int r, List<String> o, boolean rateLimited, boolean disabled, boolean serial, boolean timed, int activeTerminals, Arrival a) {
        ArrayList<Double> w = new ArrayList<Double>();
        for (String s : o)
            w.add(Double.parseDouble(s));

        this.benchmarkName = benchmarkName;
        this.id = id;
        this.time = t;
        this.rate = r;
        this.weights = Collections.unmodifiableList(w);
        this.num_weights = this.weights.size();
        this.rateLimited = rateLimited;
        this.disabled = disabled;
        this.serial = serial;
        this.timed = timed;
        this.nextSerial = 1;
        this.activeTerminals = activeTerminals;
        this.arrival = a;
    }

    boolean isRateLimited() {
        return rateLimited;
    }

    boolean isDisabled() {
        return disabled;
    }

    boolean isSerial() {
        return serial;
    }

    private boolean isTimed() {
        return timed;
    }

    boolean isLatencyRun() {
        return !timed && serial;
    }

    boolean isThroughputRun() {
        return !isLatencyRun();
    }

    void resetSerial() {
        this.nextSerial = 1;
    }

    int getActiveTerminals() {
        return activeTerminals;
    }

    int getWeightCount() {
        return (this.num_weights);
    }

    private List<Double> getWeights() {
        return (this.weights);
    }

    /**
     * Offset the test duration of the OLAP workers to adjust for the artificial
     * delay introduced when sleeping for the first minute of the test.
     *
     * This ensures that both OLAP and OLTP workers finish at the same time.
     * The sleep duration is assumed to be one minute in the OLAPWorkerThread class.
     *
     * @param isTPCC Boolean value indicating if the current worker is a TPCCWorker.
     */
    void offsetTime(boolean isTPCC){
        if (!isTPCC)
            this.time = this.time - 60;

        assert this.time > 0;
    }

    /**
     * Determine the next transaction.
     */
    int chooseTransaction(boolean isColdQuery, Worker worker) {
        if (isDisabled())
            return -1;

        if (isSerial()) {
            int ret;
            synchronized (this) {
                ret = this.nextSerial;

                // Serial runs should not execute queries with non-positive
                // weights.
                while (ret <= this.num_weights && weights.get(ret - 1) <= 0.0)
                    ret = ++this.nextSerial;

                // If it's a cold execution, then we don't want to advance yet,
                // since the hot run needs to execute the same query.
                if (!isColdQuery) {
                    // For timed, serial executions, we're doing a QPS (query
                    // throughput) run, so we loop through the list multiple
                    // times. Note that we do the modulus before the increment
                    // so that we end up in the range [1,num_weights]
                    if (isTimed()) {
                        assert this.isThroughputRun();
                        this.nextSerial %= this.num_weights;
                    }

                    ++this.nextSerial;
                }
            }
            return ret;
        } else {
            // HTAPB:
            if (worker instanceof TPCCWorker) {
                int randomPercentage = gen.nextInt(100) + 1;
                double weight = 0.0;
                for (int i = 0; i < this.num_weights; i++) {
                    weight += weights.get(i);
                    if (randomPercentage <= weight) {
                        return i + 1;
                    }
                } // FOR
            } else if (worker instanceof TPCHWorker) {
                int randomPercentage = gen.nextInt(100) + 101;
                //int randomPercentage = RandomParameters.randBetween(101, 200)+1;
                double weight = 0.0;
                for (int i = 0; i < this.num_weights; i++) {
                    weight += weights.get(i);
                    if (randomPercentage <= weight) {
                        return i + 1;
                    }
                } // FOR

            }
        }

        return -1;
    }

    /**
     * Returns a string for logging purposes when entering the phase.
     */
    String currentPhaseString() {
        String retString = "[Starting Phase] [Workload= " + benchmarkName + "] ";
        if (isDisabled()) {
            retString += "[Disabled= true]";
        } else {
            if (isLatencyRun()) {
                retString += "[Serial= true] [Time= n/a] ";
            } else {
                retString += "[Serial= " + (isSerial() ? "true" : "false")
                        + "] [Time= " + time + "] ";
            }
            retString += "[Rate= " + (isRateLimited() ? rate : "unlimited") + "] [Arrival= " + arrival + "] [Ratios= " + getWeights() + "] [Active Workers=" + getActiveTerminals() + "]";
        }
        return retString;
    }

}