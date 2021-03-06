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
package pt.haslab.htapbench.types;

public enum TransactionStatus {
    /**
     * The transaction executed successfully and committed without
     * any errors or interruptions.
     */
    SUCCESS,

    /**
     * The transaction executed successfully but then was aborted
     * due to a valid user control code. This is not an error.
     */
    INTERRUPTED,

    /**
     * The transaction did not execute due to internal benchmark
     * state. The same transaction should be retried.
     */
    RETRY,

    /**
     * The transaction did not execute due to an exception being
     * thrown from the transaction. The Worker should continue by
     * selecting a new random transaction to execute.
     */
    ABORTED,

    /**
     * The transaction executed successfully but did not return
     * a valid result set. At least one row should (usually) be
     * returned and results with a single NULL value are invalid.
     * This status only applies to TPC-H queries.
     */
    INVALID_RESULT,

    /**
     * The transaction resulted in an unknown SQLException.
     */
    UNKNOWN_EXCEPTION
}
