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
package pt.haslab.htapbench.random;

import pt.haslab.htapbench.benchmark.HTAPBConstants;
import pt.haslab.htapbench.random.distributions.Distribution;
import pt.haslab.htapbench.random.distributions.HotspotDistribution;
import pt.haslab.htapbench.random.distributions.UniformDistribution;

import java.sql.Timestamp;
import java.util.*;


/**
 * This class presents a Random Parameter Generator. This is a class that used both for
 * feeding the populate stage of the benchmark and whenever random parameters are needed
 * to build random queries. The parameters are used to bound the values generated.
 */
public class RandomParameters {

    private static final Map<String, String> nationToRegion = new LinkedHashMap<>();
    private static final Map<String, List<String>> regionToNations = new LinkedHashMap<>();
    private static final List<String> acceptableRegions = new ArrayList<String>();
    private static final List<String> nations;

    private static List<String> regions = Arrays.asList("Africa", "Asia", "Europe", "North America", "Oceania", "South America");
    private static List<Character> alphabet = Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z');
    private static List<String> su_comment = Arrays.asList("good", "bad");

    private static int regionCount = 0;

    static {
        // The following pairs of nations and regions are *not* randomly selected from,
        // in order to have the highest possible representation from all regions. The
        // nations are selected using an alternating region strategy when the benchmark
        // is generating data. This ensures that all nations are represented as well,
        // so that empty result-sets are avoided.
        nationToRegion.put("Algeria","Africa");
        nationToRegion.put("Bangladesh","Asia");
        nationToRegion.put("Austria","Europe");
        nationToRegion.put("Bahamas","North America");
        nationToRegion.put("Australia","Oceania");
        nationToRegion.put("Argentina","South America");

        nationToRegion.put("Cameroon","Africa");
        nationToRegion.put("China","Asia");
        nationToRegion.put("Belgium","Europe");
        nationToRegion.put("Canada","North America");
        nationToRegion.put("Fiji","Oceania");
        nationToRegion.put("Brazil","South America");

        nationToRegion.put("Ethiopia","Africa");
        nationToRegion.put("India","Asia");
        nationToRegion.put("Denmark","Europe");
        nationToRegion.put("Cuba","North America");
        nationToRegion.put("Marshall Islands","Oceania");
        nationToRegion.put("Chile","South America");

        nationToRegion.put("Kenya","Africa");
        nationToRegion.put("Indonesia","Asia");
        nationToRegion.put("Finland","Europe");
        nationToRegion.put("Guatemala","North America");
        nationToRegion.put("Nauru","Oceania");
        nationToRegion.put("Colombia","South America");

        nationToRegion.put("Madagascar","Africa");
        nationToRegion.put("Japan","Asia");
        nationToRegion.put("Germany","Europe");
        nationToRegion.put("Haiti","North America");
        nationToRegion.put("New Zealand","Oceania");
        nationToRegion.put("Ecuador","South America");

        nationToRegion.put("Nigeria","Africa");
        nationToRegion.put("South Korea","Asia");
        nationToRegion.put("Greece","Europe");
        nationToRegion.put("Honduras","North America");
        nationToRegion.put("Papua New Guinea","Oceania");
        nationToRegion.put("Panama","South America");

        nationToRegion.put("Rwanda","Africa");
        nationToRegion.put("Malaysia","Asia");
        nationToRegion.put("Italy","Europe");
        nationToRegion.put("Jamaica","North America");
        nationToRegion.put("Samoa","Oceania");
        nationToRegion.put("Paraguay","South America");

        nationToRegion.put("South Africa","Africa");
        nationToRegion.put("Mongolia","Asia");
        nationToRegion.put("Norway","Europe");
        nationToRegion.put("Mexico","North America");
        nationToRegion.put("Solomon Islands","Oceania");
        nationToRegion.put("Peru","South America");

        nationToRegion.put("Tanzania","Africa");
        nationToRegion.put("Saudi Arabia","Asia");
        nationToRegion.put("Spain","Europe");
        nationToRegion.put("Puerto Rico","North America");
        nationToRegion.put("Tonga","Oceania");
        nationToRegion.put("Uruguay","South America");

        nationToRegion.put("Zimbabwe","Africa");
        nationToRegion.put("Singapore","Asia");
        nationToRegion.put("United Kingdom","Europe");
        nationToRegion.put("United States","North America");
        nationToRegion.put("Tuvalu","Oceania");
        nationToRegion.put("Venezuela","South America");

        // List used to associate a key with a nation
        nations = new ArrayList<>(nationToRegion.keySet());
    }

    private String distributionType;
    private int warehouses;

    public RandomParameters(String distributionType, int warehouses) {
        this.distributionType = distributionType;
        this.warehouses = warehouses;

        // Populate the acceptable regions list and the map defining which nations
        // correspond to a given region.
        for (String region : regions) {
            if (regionCount < warehouses) {
                List<String> list = new ArrayList<String>();
                for (Map.Entry<String, String> entry : nationToRegion.entrySet()) {
                    if (entry.getValue().equals(region))
                        list.add(entry.getKey());
                }
                regionToNations.put(region, list);
                acceptableRegions.add(region);
                regionCount++;
            }
        }
    }

    public static long convertDateToLong(int year, int month, int day) {
        Timestamp ts = new Timestamp(year - 1900, month - 1, day, 0, 0, 0, 0);
        return ts.getTime();
    }

    public static long addMonthsToDate(long ts, int months) {
        Date date = new Date(ts);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        return cal.getTime().getTime();
    }

    public static int randBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public static double randDoubleBetween(int start, int end) {
        return start + Math.random() * (end - start);
    }

    public static int getDistrictNationKey(int nationkey, int id) {
        String region = nationToRegion.get(nations.get(nationkey - 1));
        List<String> nationsInRegion = regionToNations.get(region);
        String newNation = nationsInRegion.get(id - 1);
        return nations.indexOf(newNation) + 1;
    }

    // ***********************************************
    //               Nation and region
    // ***********************************************

    /**
     * Returns a random nationkey within a region that is guaranteed to
     * be a part of the generated initial dataset.
     *
     * @return a random nationkey
     */
    public int getRandomNationKey() {
        // The universe of acceptable keys are bounded by the minimum value
        // of regions.size() or the number of warehouses. For each region,
        // there should be configDistPerWhse number of nations.
        int regionIndex = RandomParameters.randBetween(0, acceptableRegions.size() - 1);
        String region = acceptableRegions.get(regionIndex);
        List<String> nationsWithinRandomRegion = regionToNations.get(region);
        int nationIndex = RandomParameters.randBetween(0, nationsWithinRandomRegion.size() - 1);
        String randomNation = nationsWithinRandomRegion.get(nationIndex);
        return nations.indexOf(randomNation) + 1;
    }

    public String getRandomRegion() {
        String nation = nations.get(getRandomNationKey() - 1);
        return nationToRegion.get(nation);
    }

    public String getRandomNation() {
        return nations.get(getRandomNationKey() - 1);
    }

    /**
     * Returns a random nation within the specified region. The input parameter
     * must be generated using the getRandomRegion() method to make sure the
     * region is represented in the benchmark.
     *
     * @param region The region to generate a nation within.
     * @return a nation within the given region.
     */
    public String getRandomNation(String region) {
        List<String> nationsWithinRegion = regionToNations.get(region);
        int nationIndex = RandomParameters.randBetween(0, nationsWithinRegion.size() - 1);
        return nationsWithinRegion.get(nationIndex);
    }

    // ***********************************************
    //                      Other
    // ***********************************************

    public Character generateRandomCharacter() {
        Distribution dist = getDistributionType(alphabet.size() - 1);
        int rand = dist.nextInt();
        return alphabet.get(rand);
    }

    public String getRandomSuComment() {
        Distribution dist = getDistributionType(su_comment.size() - 1);
        int rand = dist.nextInt();
        return su_comment.get(rand);
    }

    public String getRandomPhoneCountryCode() {
        Distribution dist = getDistributionType(nations.size() - 1);
        int rand = dist.nextInt() + 10;
        return Integer.toString(rand);
    }

    private Distribution getDistributionType(int size) {
        Distribution dist = null;

        if (distributionType.equals("uniform")) {
            dist = new UniformDistribution(1, size);
        }

        if (distributionType.equals("hotspot")) {
            int lower_bound = 1;
            double hotsetFraction = 0.5;
            double hotOpnFraction = 0.5;
            dist = new HotspotDistribution(lower_bound, size, hotsetFraction, hotOpnFraction);
        }

        return dist;
    }

    /**
     * The exponential distribution used by the TPC, for instance to calculate the
     * transaction's thinktime.
     *
     * @param rand the random generator
     * @param max  the maximum number which could be accept for this distribution
     * @param lMax the maximum number which could be accept for the following
     *             execution rand.nexDouble
     * @param mu   the base value provided to calculate the exponential number.
     *             For instance, it could be the mean thinktime
     * @return the calculated exponential number
     */
    public static long negExp(Random rand, long max, double lMax, double mu) {
        double r = rand.nextDouble();

        if (r < lMax) {
            return (max);
        }
        return ((long) (-mu * Math.log(r)));

    }
}
