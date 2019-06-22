
/**
 * Copyright 2015 by OLTPBenchmark Project                                   *
 * *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 * *
 * http://www.apache.org/licenses/LICENSE-2.0                              *
 * *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 * *****************************************************************************
 * /*
 * Copyright 2017 by INESC TEC
 * This work was based on the OLTPBenchmark Project
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * A very nice and simple generic Histogram
 *
 * @author svelagap
 * @author pavlo
 */
/**
 * A very nice and simple generic Histogram
 * @author svelagap
 * @author pavlo
 */
package pt.haslab.htapbench.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public abstract class FileUtil {

    private static final Logger LOG = Logger.getLogger(FileUtil.class);
    private static final Pattern EXT_SPLIT = Pattern.compile("\\.");

    /**
     * Join path components
     */
    public static String joinPath(String... args) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String a : args) {
            if (a != null && a.length() > 0) {
                if (!first) {
                    result.append("/");
                }
                result.append(a);
                first = false;
            }
        }
        return result.toString();
    }

    /**
     * Given a basename for a file, find the next possible filename if this file
     * already exists. For example, if the file test.res already exists, create
     * a file called, test.1.res
     */
    public static String getNextFilename(String basename) {

        if (!exists(basename))
            return basename;

        File f = new File(basename);
        if (f.isFile()) {
            String parts[] = EXT_SPLIT.split(basename);

            // Check how many files already exist
            int counter = 1;
            String nextName = parts[0] + "." + counter + "." + parts[1];
            while (exists(nextName)) {
                ++counter;
                nextName = parts[0] + "." + counter + "." + parts[1];
            }
            return nextName;
        }


        // Should we throw instead??
        return null;
    }

    public static boolean exists(String path) {
        return (new File(path).exists());
    }


    /**
     * Resolves the possibly relative path to the absolute
     * and platform-specific path.
     *
     * @param path filepath to resolve.
     * @return the resolved filepath.
     */
    public static String resolvePath(String path){
        try {
            File filepath = new File(path);
            return filepath.getCanonicalPath();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create a directory if the directory does not already exist.
     *
     * @param path directory that should be created.
     * @return boolean value indicating if the directory is empty after this procedure ended.
     */
    public static boolean makeDirIfNotExists(String path) {
        File f = new File(path);

        // Create the directory if it does not exist
        if (!f.exists() && !f.mkdirs())
            throw new RuntimeException("The path " + path + " failed to be created");

        // Determine if the directory contains data
        if (f.list().length > 0)
            return false;

        // The directory exists and is empty
        return true;
    }

    static void writeStringToFile(File file, String content) throws IOException {
        FileWriter writer = new FileWriter(file);
        writer.write(content);
        writer.flush();
        writer.close();
    }

    static String readFile(String path) {
        StringBuilder buffer = new StringBuilder();
        try {
            BufferedReader in = FileUtil.getReader(path);
            while (in.ready()) {
                buffer.append(in.readLine()).append("\n");
            } // WHILE
            in.close();
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read file contents from '" + path + "'", ex);
        }
        return (buffer.toString());
    }

    /**
     * Creates a BufferedReader for the given input path Can handle both gzip
     * and plain text files
     */
    private static BufferedReader getReader(String path) throws IOException {
        return (FileUtil.getReader(new File(path)));
    }

    /**
     * Creates a BufferedReader for the given input path Can handle both gzip
     * and plain text files
     */
    private static BufferedReader getReader(File file) throws IOException {
        if (!file.exists()) {
            throw new IOException("The file '" + file + "' does not exist");
        }

        BufferedReader in;
        if (file.getPath().endsWith(".gz")) {
            FileInputStream fin = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fin);
            in = new BufferedReader(new InputStreamReader(gzis));
            LOG.debug("Reading in the zipped contents of '" + file.getName() + "'");
        } else {
            in = new BufferedReader(new FileReader(file));
            LOG.debug("Reading in the contents of '" + file.getName() + "'");
        }
        return (in);
    }
}
