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

    private static List<String> nations = Arrays.asList("Australia", "Belgium", "Camaroon", "Denmark", "Ecuador", "France", "Germany", "Hungary", "Italy", "Japan", "Kenya", "Lithuania", "Mexico",
            "Netherlands", "Oman", "Portugal", "Qatar", "Rwanda", "Serbia", "Togo", "United States", "Vietman", "Singapore", "Cambodia", "Yemen", "Zimbabwe",
            "Argentina", "Bolivia", "Canada", "Dominican Republic", "Egypt", "Finnland", "Ghana", "Haiti", "India", "Jamaica", "kazahkstan", "Luxembourg", "Morocco",
            "Norway", "Poland", "Peru", "Nicaragua", "Romania", "South Africa", "Thailand", "United Kingdom", "Venezuela", "Liechtenstei", "Austria", "Laos", "Zambia",
            "Switzerland", "China", "Papua New Guinea", "East Timor", "Bulgaria", "Brazil", "Albania", "Andorra", "Belize", "Botswana");

    private static List<String> regions = Arrays.asList("Africa", "America", "Asia", "Australia", "Europe");
    private static List<Character> alphabet = Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z');
    private static List<String> su_comment = Arrays.asList("good", "bad");

    private String distributionType = null;

    public RandomParameters(String distributionType) {
        this.distributionType = distributionType;
    }

    public RandomParameters() {
    }

    public static long convertDateToLong(int year, int month, int day) {
        Timestamp ts = new Timestamp(year, month, day, 0, 0, 0, 0);
        return ts.getTime();
    }

    public static long addMonthsToDate(long ts, int months) {
        Date date = new Date(ts);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        return cal.getTime().getTime();
    }

    public String generateRandomCharacter() {
        Distribution dist = getDistributionType(alphabet.size() - 1);
        int rand = dist.nextInt();
        return "" + alphabet.get(rand);
    }

    public static int randBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public static double randDoubleBetween(int start, int end) {
        return start + Math.random() * (end - start);
    }

    public String getRandomNation() {
        Distribution dist = getDistributionType(nations.size() - 1);
        int rand = dist.nextInt();
        return nations.get(rand);
    }

    public String getRandomRegion() {
        Distribution dist = getDistributionType(regions.size() - 1);
        int rand = dist.nextInt();
        return regions.get(rand);
    }

    public String getRandomSuComment() {
        Distribution dist = getDistributionType(su_comment.size());
        int rand = dist.nextInt();
        return su_comment.get(rand);
    }

    public String getRandomPhoneCountryCode() {
        Distribution dist = getDistributionType(nations.size() - 1);
        int rand = dist.nextInt() + 10;
        return "" + rand;
    }

    private Distribution getDistributionType(int size) {
        Distribution dist = null;

        if (distributionType.equals("uniform")) {
            dist = new UniformDistribution(1, size - 1);
        }

        if (distributionType.equals("hotspot")) {
            int lower_bound = 1;
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
