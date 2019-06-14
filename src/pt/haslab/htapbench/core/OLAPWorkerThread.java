
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

import pt.haslab.htapbench.api.Worker;
import pt.haslab.htapbench.util.QueueLimitException;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OLAPWorkerThread implements Runnable {

    private List<Worker> workers;
    private WorkloadConfiguration workConf;
    private int intervalMonitor;
    private Results results;

    OLAPWorkerThread(List<Worker> workers, WorkloadConfiguration workConf, int intervalMonitoring) {
        this.workers = workers;
        this.workConf = workConf;
        this.intervalMonitor = intervalMonitoring;
    }

    @Override
    public void run() {
        try {
            // The first few minutes should consist of only OLTP workload
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
