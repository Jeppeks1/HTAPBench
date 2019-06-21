
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

public class OLTPWorkerThread implements Runnable{
    
    List<Worker> workers;
    List<WorkloadConfiguration> workConfs;
    int intervalMonitor;
    boolean calibrate;
    Results results;
    private Clock clock;

    public OLTPWorkerThread(List<Worker> workers, List<WorkloadConfiguration> workConfs, int intervalMonitoring, boolean calibrate){
        this.workers=workers;
        this.workConfs=workConfs;
        this.intervalMonitor=intervalMonitoring;
        this.calibrate=calibrate;
    }
    
    @Override
    public void run() {
        results = ThreadBench.runRateLimitedOLTP(workers, workConfs, intervalMonitor, calibrate);
    }
    
    public Results getResults(){
        return results;
    }
    
}
