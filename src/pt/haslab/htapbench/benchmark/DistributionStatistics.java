
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
package pt.haslab.htapbench.benchmark;

import java.util.Arrays;

public class DistributionStatistics {
    private static final double[] PERCENTILES = {0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0};

    private static final int MINIMUM = 0;
    private static final int PERCENTILE_25TH = 1;
    private static final int MEDIAN = 2;
    private static final int PERCENTILE_75TH = 3;
    private static final int PERCENTILE_90TH = 4;
    private static final int PERCENTILE_95TH = 5;
    private static final int PERCENTILE_99TH = 6;
    private static final int MAXIMUM = 7;

    private final int count;
    private final long[] percentiles;
    private final double average;
    private final double standardDeviation;

    private DistributionStatistics(int count, long[] percentiles, double average, double standardDeviation) {
        assert count > 0;
        assert percentiles.length == PERCENTILES.length;
        this.count = count;
        this.percentiles = Arrays.copyOfRange(percentiles, 0, PERCENTILES.length);
        this.average = average;
        this.standardDeviation = standardDeviation;
    }

    /**
     * Computes distribution statistics over values. WARNING: This will sort values.
     */
    public static DistributionStatistics computeStatistics(long[] values) {
        if (values.length == 0) {
            long[] percentiles = new long[PERCENTILES.length];
            for (int i = 0; i < percentiles.length; ++i) {
                percentiles[i] = -1;
            }

            return new DistributionStatistics(values.length, percentiles, 0, 0);
        }

        Arrays.sort(values);

        double sum = 0;
        for (long value1 : values) {
            sum += value1;
        }

        double average = sum / values.length;
        double sumDiffsSquared = 0;
        double standardDeviation = 0;

        for (long value : values) {
            double v = value - average;
            sumDiffsSquared += v * v;
        }

        if (values.length > 1) {
            standardDeviation = Math.sqrt(sumDiffsSquared / (values.length - 1));
        }

        // NOTE: NIST recommends interpolating. This just selects the closest value,
        // which is described as another common technique.
        // http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.html
        long[] percentiles = new long[PERCENTILES.length];
        for (int i = 0; i < percentiles.length; ++i) {
            int index = (int) (PERCENTILES[i] * values.length);
            if (index == values.length)
                index = values.length - 1;
            percentiles[i] = values[index];
        }

        return new DistributionStatistics(values.length, percentiles, average, standardDeviation);
    }

    public int getCount() {
        return count;
    }

    double getAverage() {
        return average;
    }

    public double getStandardDeviation() {
        return standardDeviation;
    }

    double getMinimum() {
        return percentiles[MINIMUM];
    }

    double get25thPercentile() {
        return percentiles[PERCENTILE_25TH];
    }

    double getMedian() {
        return percentiles[MEDIAN];
    }

    double get75thPercentile() {
        return percentiles[PERCENTILE_75TH];
    }

    double get90thPercentile() {
        return percentiles[PERCENTILE_90TH];
    }

    double get95thPercentile() {
        return percentiles[PERCENTILE_95TH];
    }

    double get99thPercentile() {
        return percentiles[PERCENTILE_99TH];
    }

    double getMaximum() {
        return percentiles[MAXIMUM];
    }

    @Override
    public String toString() {
        // convert times to ms
        return "[min=" + getMinimum() / 1e6 + ", " + "25th="
                + get25thPercentile() / 1e6 + ", " + "median="
                + getMedian() / 1e6 + ", " + "avg=" + getAverage() / 1e6 + ", "
                + "75th=" + get75thPercentile() / 1e6 + ", " + "90th="
                + get90thPercentile() / 1e6 + ", " + "95th="
                + get95thPercentile() / 1e6 + ", " + "99th="
                + get99thPercentile() / 1e6 + ", " + "max=" + getMaximum()
                / 1e6 + "]";
    }
}
