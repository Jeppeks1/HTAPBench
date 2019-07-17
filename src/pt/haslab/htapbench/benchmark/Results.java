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

import java.io.PrintStream;
import java.util.*;

import pt.haslab.htapbench.benchmark.LatencyRecord.Sample;
import pt.haslab.htapbench.api.TransactionType;
import pt.haslab.htapbench.util.Histogram;

public final class Results {
    private final long nanoSeconds;
    private final int measuredRequests;
    public final DistributionStatistics latencyDistribution;
    public final Histogram<TransactionType> txnSuccess = new Histogram<TransactionType>(true);
    public final Histogram<TransactionType> txnAborted = new Histogram<TransactionType>(true);
    public final Histogram<TransactionType> txnRetry = new Histogram<TransactionType>(true);
    public final Histogram<TransactionType> txnErrors = new Histogram<TransactionType>(true);
    public final Map<TransactionType, Histogram<String>> txnRecordedMessages = new HashMap<TransactionType, Histogram<String>>();
    private int ts_counter = 0;
    private String name;

    private final List<LatencyRecord.Sample> latencySamples;

    public Results(long nanoSeconds, int measuredRequests, DistributionStatistics latencyDistribution, final List<LatencyRecord.Sample> latencySamples) {
        this.nanoSeconds = nanoSeconds;
        this.measuredRequests = measuredRequests;
        this.latencyDistribution = latencyDistribution;

        if (latencyDistribution == null) {
            assert latencySamples == null;
            this.latencySamples = null;
        } else {
            // defensive copy
            this.latencySamples = Collections.unmodifiableList(new ArrayList<LatencyRecord.Sample>(latencySamples));
            assert !this.latencySamples.isEmpty();
        }
    }

    /**
     * Get a histogram of how often each transaction was executed
     */
    public final Histogram<TransactionType> getSuccessHistogram() {
        return (this.txnSuccess);
    }

    public final Histogram<TransactionType> getRetryHistogram() {
        return (this.txnRetry);
    }

    public final Histogram<TransactionType> getAbortedHistogram() {
        return (this.txnAborted);
    }

    public final Histogram<TransactionType> getErrorHistogram() {
        return (this.txnErrors);
    }

    public final Map<TransactionType, Histogram<String>> getRecordedMessagesHistogram() {
        return (this.txnRecordedMessages);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public double getRequestsPerSecond() {
        return (double) measuredRequests / (double) nanoSeconds * 1e9;
    }

    private double get_QphH() {
        return (double) (measuredRequests) / (nanoSeconds * 0.000000001 / 3600);
    }

    private double get_tpmC() {
        return measuredRequests / (nanoSeconds * 0.000000001 / 60) * 0.45;
    }

    @Override
    public String toString() {
        String results = "Results(nanoSeconds=" + nanoSeconds + ", measuredRequests=" + measuredRequests + ") = " +
                getRequestsPerSecond() + " requests/sec \n************************** \n" +
                "Total TS Count: " + this.ts_counter + "\n";

        if (getName().equals("TPCC"))
            results = results + "tpmC : " + get_tpmC() + "\n**************************";

        if (getName().equals("TPCH"))
            results = results + "QphH : " + get_QphH() + "\n**************************";

        return results;
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out) {
        writeCSV(windowSizeSeconds, out, TransactionType.INVALID);
    }

    public void writeCSV(int windowSizeSeconds, PrintStream out, TransactionType txType) {
        boolean tpcc = this.getName().equalsIgnoreCase("TPCC");

        String headerTPCC = "time,    TPS, avg_lat, min_lat, 25th_lat, 50th_lat, 75th_lat, 90th_lat, 95th_lat, 99th_lat,  max_lat";
        String headerTPCH = "time, queries, avg_lat, min_lat, 25th_lat, 50th_lat, 75th_lat, 90th_lat, 95th_lat, 99th_lat,  max_lat";

        String formatTPCC = "%4d, %6.2f, %7.2f, %7.2f, %8.2f, %8.2f, %8.2f, %8.2f, %8.2f, %8.2f, %8.2f\n";
        String formatTPCH = "%4d, %7d, %7.0f, %7.0f, %8.0f, %8.0f, %8.0f, %8.0f, %8.0f, %8.0f, %8.0f\n";

        out.println("**********************************************");
        out.println(this.getName() + " results (latencies in milliseconds)");
        out.println("**********************************************");
        out.println(tpcc ? headerTPCC : headerTPCH);

        int i = 0;
        final double factor = 1000;
        for (DistributionStatistics s : new TimeBucketIterable(latencySamples, windowSizeSeconds, txType)) {
            if (tpcc){
                out.printf(Locale.US, formatTPCC,
                        i * windowSizeSeconds,
                        (double) s.getCount() / windowSizeSeconds,
                        s.getAverage() / factor,
                        s.getMinimum() / factor,
                        s.get25thPercentile() / factor,
                        s.getMedian() / factor,
                        s.get75thPercentile() / factor,
                        s.get90thPercentile() / factor,
                        s.get95thPercentile() / factor,
                        s.get99thPercentile() / factor,
                        s.getMaximum() / factor);
            } else {
                out.printf(Locale.US, formatTPCH,
                        i * windowSizeSeconds,
                        s.getCount(),
                        s.getAverage() / factor,
                        s.getMinimum() / factor,
                        s.get25thPercentile() / factor,
                        s.getMedian() / factor,
                        s.get75thPercentile() / factor,
                        s.get90thPercentile() / factor,
                        s.get95thPercentile() / factor,
                        s.get99thPercentile() / factor,
                        s.getMaximum() / factor);
            }

            i += 1;
        }

        out.println(toString());
    }

    public void writeAllCSVAbsoluteTiming(PrintStream out) {
        // Write the ClientBalancer samples formatted as a result file
        if (this.getName().equals("ClientBalancer")) {
            out.println("**********************************************");
            out.println(this.getName() + " Results:");
            out.println("**********************************************");
            out.println("time,    TPS, OLAP workers");

            // Write the results. The variable names in the Sample do not match their
            // meaning, as the Sample class was lazily adopted to fit this purpose.
            for (Sample s : latencySamples) {
                out.printf(Locale.US, "%4d, %6.2f, %12d\n", s.startUs, (double) s.workerId / s.phaseId, s.tranType);
            }

            out.println(toString());
        } else {
            // Otherwise write the raw result file
            if (this.getName().endsWith("TPCC"))
                out.println("transaction type (index in config file), start time (us), end time(us), latency(us), worker id(start number), phase id(index in config file)");

            if (this.getName().equals("TPCH"))
                out.println("transaction type (index in config file), start time (us), end time(us), latency(us), worker id(start number), Rows in ResultSet");

            for (Sample s : latencySamples) {
                out.println(s.tranType + "," + s.startUs + "," + s.endUs + "," + s.latencyUs + "," + s.workerId + "," + s.phaseId);
            }
        }
    }

    public void setTsCounter(int ts_counter) {
        this.ts_counter = ts_counter;
    }

    /**
     * Combines the histograms in this Results and the histograms
     * in the other Results.
     *
     * @param other the Results to be combined with this.
     * @return the Results in this with the histograms combined.
     */
    public Results combineHistograms(Results other){
        // Return this instance of the Results if other is null
        if (other == null)
            return this;

        // Combine the histograms
        getSuccessHistogram().putHistogram(other.getSuccessHistogram());
        getErrorHistogram().putHistogram(other.getErrorHistogram());
        getAbortedHistogram().putHistogram(other.getAbortedHistogram());
        getRetryHistogram().putHistogram(other.getRetryHistogram());

        // Combine the histograms within the maps containing recorded messages
        for (TransactionType key : other.txnRecordedMessages.keySet()){
            if (txnRecordedMessages.containsKey(key)){
                // Combine values
                txnRecordedMessages.get(key).putHistogram(other.txnRecordedMessages.get(key));
            } else {
                // Add new entry
                txnRecordedMessages.put(key, other.txnRecordedMessages.get(key));
            }
        }

        return this;
    }
}