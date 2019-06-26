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
    private static final List<String> nations;

    static {
        nationToRegion.put("Algeria","Africa");
        nationToRegion.put("Cameroon","Africa");
        nationToRegion.put("Ethiopia","Africa");
        nationToRegion.put("Kenya","Africa");
        nationToRegion.put("Madagascar","Africa");
        nationToRegion.put("Nigeria","Africa");
        nationToRegion.put("Rwanda","Africa");
        nationToRegion.put("South Africa","Africa");
        nationToRegion.put("Tanzania","Africa");
        nationToRegion.put("Zimbabwe","Africa");
        nationToRegion.put("Bangladesh","Asia");
        nationToRegion.put("China","Asia");
        nationToRegion.put("India","Asia");
        nationToRegion.put("Indonesia","Asia");
        nationToRegion.put("Japan","Asia");
        nationToRegion.put("South Korea","Asia");
        nationToRegion.put("Malaysia","Asia");
        nationToRegion.put("Mongolia","Asia");
        nationToRegion.put("Saudi Arabia","Asia");
        nationToRegion.put("Singapore","Asia");
        nationToRegion.put("Taiwan","Asia");
        nationToRegion.put("Thailand","Asia");
        nationToRegion.put("Turkey","Asia");
        nationToRegion.put("Vietnam","Asia");
        nationToRegion.put("Yemen","Asia");
        nationToRegion.put("Austria","Europe");
        nationToRegion.put("Belgium","Europe");
        nationToRegion.put("Czech Republic","Europe");
        nationToRegion.put("Denmark","Europe");
        nationToRegion.put("Finland","Europe");
        nationToRegion.put("France","Europe");
        nationToRegion.put("Germany","Europe");
        nationToRegion.put("Greece","Europe");
        nationToRegion.put("Italy","Europe");
        nationToRegion.put("Netherlands","Europe");
        nationToRegion.put("Norway","Europe");
        nationToRegion.put("Russia","Europe");
        nationToRegion.put("Spain","Europe");
        nationToRegion.put("Sweden","Europe");
        nationToRegion.put("Ukraine","Europe");
        nationToRegion.put("United Kingdom","Europe");
        nationToRegion.put("Canada","North America");
        nationToRegion.put("Cuba","North America");
        nationToRegion.put("Guatemala","North America");
        nationToRegion.put("Honduras","North America");
        nationToRegion.put("Mexico","North America");
        nationToRegion.put("United States","North America");
        nationToRegion.put("Australia","Oceania");
        nationToRegion.put("Fiji","Oceania");
        nationToRegion.put("New Zealand","Oceania");
        nationToRegion.put("Papua New Guinea","Oceania");
        nationToRegion.put("Samoa","Oceania");
        nationToRegion.put("Argentina","South America");
        nationToRegion.put("Brazil","South America");
        nationToRegion.put("Chile","South America");
        nationToRegion.put("Ecuador","South America");
        nationToRegion.put("Paraguay","South America");
        nationToRegion.put("Peru","South America");
        nationToRegion.put("Uruguay","South America");
        nationToRegion.put("Venezuela","South America");

        nations = new ArrayList<>(nationToRegion.keySet());
    }

    private static List<String> regions = Arrays.asList("Africa", "Asia", "Europe", "North America", "Oceania", "South America");
    private static List<Character> alphabet = Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z');
    private static List<String> su_comment = Arrays.asList("good", "bad");

    private String distributionType;
    private int regionCount = 0;
    private int nationCount = 0;
    private int warehouses;

    public RandomParameters(String distributionType, int warehouses) {
        this.distributionType = distributionType;
        this.warehouses = warehouses;
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

    public String getRandomRegion() {
        int bound = warehouses > regions.size() ? regions.size() : warehouses;
        Distribution dist = getDistributionType(bound);

        // Make sure as many regions are represented as possible
        if (regionCount < bound)
            return regions.get(++regionCount);
        else
            return regions.get(dist.nextInt());
    }

    public String getRandomNation() {
        int bound = warehouses > nations.size() ? nations.size() : warehouses;
        Distribution dist = getDistributionType(bound);

        // Make sure as many nations are represented as possible
        if (nationCount < bound)
            return nations.get(++nationCount);
        else
            return nations.get(dist.nextInt());
    }

    public String getRandomNation(String region) {
        while (true) {
            String nation = getRandomNation();
            if (nationToRegion.get(nation).equals(region))
                return nation;
        }
    }

    public String getRegion(String nation) {
        return nationToRegion.get(nation);
    }

    public Character generateRandomCharacter() {
        Distribution dist = getDistributionType(alphabet.size());
        int rand = dist.nextInt();
        return alphabet.get(rand);
    }

    public String getRandomSuComment() {
        Distribution dist = getDistributionType(su_comment.size());
        int rand = dist.nextInt();
        return su_comment.get(rand);
    }

    public String getRandomPhoneCountryCode() {
        Distribution dist = getDistributionType(nations.size());
        int rand = dist.nextInt() + 10;
        return Integer.toString(rand);
    }

    private Distribution getDistributionType(int size) {
        Distribution dist = null;

        if (distributionType.equals("uniform")) {
            dist = new UniformDistribution(0, size - 1);
        }

        if (distributionType.equals("hotspot")) {
            int lower_bound = 0;
            int upper_bound = size - 1;
            double hotsetFraction = 0.5;
            double hotOpnFraction = 0.5;
            dist = new HotspotDistribution(lower_bound, upper_bound, hotsetFraction, hotOpnFraction);
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
