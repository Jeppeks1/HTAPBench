/*
 * Copyright 2017 by INESC TEC                                               
 * Developed by Fábio Coelho                                                 
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
package pt.haslab.htapbench.util;

import java.io.IOException;

import pt.haslab.htapbench.util.json.JSONException;
import pt.haslab.htapbench.util.json.JSONObject;
import pt.haslab.htapbench.util.json.JSONString;
import pt.haslab.htapbench.util.json.JSONStringer;

public interface JSONSerializable extends JSONString {
    public void save(String output_path) throws IOException;
    public void load(String input_path) throws IOException;
    public void toJSON(JSONStringer stringer) throws JSONException;
    public void fromJSON(JSONObject json_object) throws JSONException;
}
