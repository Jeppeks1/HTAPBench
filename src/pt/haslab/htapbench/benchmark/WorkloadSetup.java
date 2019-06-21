
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

import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

/**
 * This class computes the correct number of warehouses (#wh) and terminals (#terminals) according
 * to the desired target TPS. To perform this computation, the ideal TPC-C client is considered.
 */
public class WorkloadSetup {

    private XMLConfiguration xmlConfig;
    private int targetTPS;
    private int warehouses;
    private int terminals;

    // Value defined in the TPC-C specification [pag. 61 - clause 4.2]
    private static final double max_tmpC = HTAPBConstants.max_tmpC;

    public WorkloadSetup(XMLConfiguration xmlConfig) {
        this.xmlConfig = xmlConfig;
        this.targetTPS = xmlConfig.getInt("target_TPS");
    }

    /**
     * Computes the workload configuration: #warehouses and #terminals according to the target TPS.
     */
    public void computeWorkloadSetup() {
        // Convert from txn per second to txn per minute and extract the newOrder weight.
        int tpm = targetTPS * 60;
        double newOrder_weight = get_NewOrder_WeightFromXMLConf();
        int target_tpmC = (int) Math.round(tpm * newOrder_weight);

        // Compute the number of terminals and warehouses according to an ideal client
        this.terminals = (int) Math.ceil(target_tpmC / max_tmpC);
        this.warehouses = (int) Math.ceil(this.terminals / 10.0);
    }

    /**
     * Returns the number of computed warehouses.
     */
    public int getWarehouses() {
        return this.warehouses;
    }

    public void setWarehouses(int warehouses) {
        this.warehouses = warehouses;
    }

    /**
     * Returns the number of computer Terminals.
     */
    public int getTerminals() {
        return this.terminals;
    }

    /**
     * Returns the TargetTPS set through the xml configuration.
     */
    public int getTargetTPS() {
        return this.targetTPS;
    }

    /**
     * Extracts the weights from the xmlConfig file whenever we want to use a different transaction mixed from the standard TPC-C specification.
     */
    private double get_NewOrder_WeightFromXMLConf() {
        SubnodeConfiguration work = xmlConfig.configurationAt("works/work[" + 1 + "]");
        String weightKey = work.getString("weights").toLowerCase();
        return Double.parseDouble("0." + weightKey);
    }
}
