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
package pt.haslab.htapbench.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler to write to and read from auxiliar file.
 */
public class AuxiliarFileHandler {

    public static void writeToFile(String file_path, long start_ts, long final_ts, int targetTPS) {
        OutputStream out = null;
        file_path = cleanFilePath(file_path);
        try {
            Properties props = new Properties();
            // To be read later
            props.setProperty("startLoadTimestamp", "" + start_ts);
            props.setProperty("lastLoadTimestamp", "" + final_ts);

            // Human readable for debugging
            props.setProperty("startTimestamp", new Timestamp(start_ts).toString());
            props.setProperty("lastTimestamp", new Timestamp(final_ts).toString());

            // Variable indicating which TPS the data was created with.
            props.setProperty("targetTPS", "" + targetTPS);

            File file = new File(file_path + "/htapb_auxiliar");
            out = new FileOutputStream(file);
            props.store(out, "HTAPB auxiliar file");
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static long importLastTs(String file_path) {
        long result = 0L;
        file_path = cleanFilePath(file_path);
        try {
            Properties props = new Properties();
            InputStream input;

            File file = new File(file_path + "/htapb_auxiliar");
            input = new FileInputStream(file);

            props.load(input);

            String res = props.getProperty("lastLoadTimestamp");
            result = Long.parseLong(res);
        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }

    public static long importFirstTs(String file_path) {
        long result = 0L;
        file_path = cleanFilePath(file_path);
        try {
            Properties props = new Properties();
            InputStream input;

            File file = new File(file_path + "/htapb_auxiliar");
            input = new FileInputStream(file);

            props.load(input);

            String res = props.getProperty("startLoadTimestamp");
            result = Long.parseLong(res);

        } catch (IOException ex) {
            Logger.getLogger(AuxiliarFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }

    /**
     * Removes the possibly trailing backslash in the input argument that
     * denotes a file path.
     *
     * @param file_path the file path which may contain a trailing backslash.
     * @return file path with no trailing backslash
     */
    private static String cleanFilePath(String file_path){
        if (file_path.charAt(file_path.length() - 1) == '/')
            return file_path.substring(0, file_path.length() - 1);

        return file_path;
    }
}
