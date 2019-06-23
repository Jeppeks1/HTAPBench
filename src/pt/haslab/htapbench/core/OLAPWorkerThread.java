
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

import pt.haslab.htapbench.benchmark.Results;
import pt.haslab.htapbench.benchmark.WorkloadConfiguration;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OLAPWorkerThread implements Runnable {

    private WorkloadConfiguration workConf;
    private List<Worker> workers;
    private Results results;

    private int intervalMonitor;

    OLAPWorkerThread(List<Worker> workers, WorkloadConfiguration workConf) {
        this.intervalMonitor = workConf.getIntervalMonitor();
        this.workConf = workConf;
        this.workers = workers;
    }

    @Override
    public void run() {
        try {
            // The first minute should consist of only an OLTP workload
            Thread.sleep(60 * 1000);
            results = ThreadBench.runThreadBench(workers, workConf, intervalMonitor);
        } catch (InterruptedException ex) {
            Logger.getLogger(OLAPWorkerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Results getResults() {
        return results;
    }

}
