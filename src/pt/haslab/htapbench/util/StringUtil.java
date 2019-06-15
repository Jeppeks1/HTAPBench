
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
 *
 * @author pavlo
 * @author Djellel
 */
/**
 * @author pavlo
 * @author Djellel
 */
package pt.haslab.htapbench.util;

import pt.haslab.htapbench.api.TransactionType;

import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public abstract class StringUtil {

    private static final Pattern LINE_SPLIT = Pattern.compile("\n");

    private static String CACHE_REPEAT_STR = null;
    private static Integer CACHE_REPEAT_SIZE = null;
    private static String CACHE_REPEAT_RESULT = null;

    /**
     * Return key/value maps into a nicely formatted table
     * Delimiter ":", No UpperCase Keys, No Boxing
     */
    public static String formatMaps(Map<?, ?>... maps) {
        return (formatMaps(":", false, false, false, false, true, true, maps));
    }

    public static String formatRecordedMessages(Map<TransactionType, Histogram<String>> map){
        // Put the map in a TreeMap for easy sorting of keys
        Map<TransactionType, Histogram<String>> treeMap = new TreeMap<TransactionType, Histogram<String>>(map);
        StringBuilder s = new StringBuilder();

        // The format should be "TransactionType [count] message"
        String str = "%-20s [%4d] %s";

        // Loop through the entries
        for (TransactionType key : treeMap.keySet()){
            Histogram<String> hist = treeMap.get(key);
            for (String msg : hist.values()){
                int count = hist.get(msg);

                if (count > 0){
                    s.append(String.format(str, key, count, StringUtil.abbrv(msg, 80)));
                    s.append('\n');
                }
            }
        }

        return s.toString();
    }

    /**
     * Return key/value maps into a nicely formatted table
     * The maps are displayed in order from first to last, and there will be a spacer
     * created between each map. The format for each record is:
     *
     * <KEY><DELIMITER><SPACING><VALUE>
     *
     * If the delimiter is an equal sign, then the format is:
     *
     *  <KEY><SPACING><DELIMITER><VALUE>
     */
    @SuppressWarnings("unchecked")
    private static String formatMaps(String delimiter, boolean upper, boolean box, boolean border_top, boolean border_bottom, boolean recursive, boolean first_element_title, Map<?, ?>... maps) {
        boolean need_divider = (maps.length > 1 || border_bottom || border_top);

        // Figure out the largest key size so we can get spacing right
        int max_key_size = 0;
        int max_title_size = 0;
        final Map<Object, String[]> map_keys[] = (Map<Object, String[]>[]) new Map[maps.length];
        final boolean map_titles[] = new boolean[maps.length];
        for (int i = 0; i < maps.length; i++) {
            Map<?, ?> m = maps[i];
            if (m == null) continue;
            Map<Object, String[]> keys = new HashMap<Object, String[]>();
            boolean first = true;
            for (Object k : m.keySet()) {
                String k_str[] = LINE_SPLIT.split(k != null ? k.toString() : "");
                keys.put(k, k_str);

                // If the first element has a null value, then we can let it be the title for this map
                // It's length doesn't affect the other keys, but will affect the total size of the map
                if (first && first_element_title && m.get(k) == null) {
                    for (String line : k_str) {
                        max_title_size = Math.max(max_title_size, line.length());
                    } // FOR
                    map_titles[i] = true;
                } else {
                    for (String line : k_str) {
                        max_key_size = Math.max(max_key_size, line.length());
                    } // FOR
                    if (first) map_titles[i] = false;
                }
                first = false;
            } // FOR
            map_keys[i] = keys;
        } // FOR

        boolean equalsDelimiter = delimiter.equals("=");
        final String f = "%-" + (max_key_size + delimiter.length() + 1) + "s" +
                (equalsDelimiter ? "= " : "") +
                "%s\n";

        // Now make StringBuilder blocks for each map
        // We do it in this way so that we can get the max length of the values
        int max_value_size = 0;
        StringBuilder blocks[] = new StringBuilder[maps.length];
        for (int map_i = 0; map_i < maps.length; map_i++) {
            blocks[map_i] = new StringBuilder();
            Map<?, ?> m = maps[map_i];
            if (m == null) continue;
            Map<Object, String[]> keys = map_keys[map_i];

            boolean first = true;
            for (Entry<?, ?> e : m.entrySet()) {
                String key[] = keys.get(e.getKey());

                if (first && map_titles[map_i]) {
                    blocks[map_i].append(StringUtil.join("\n", key));
                    if (!CollectionUtil.last(key).endsWith("\n")) blocks[map_i].append("\n");

                } else {
                    Object v_obj = e.getValue();
                    String v;
                    if (recursive && v_obj instanceof Map<?, ?>) {
                        v = formatMaps(delimiter, upper, box, border_top, border_bottom, recursive, first_element_title, (Map<?, ?>) v_obj).trim();
                    } else if (key.length == 1 && key[0].trim().isEmpty() && v_obj == null) {
                        blocks[map_i].append("\n");
                        continue;
                    } else if (v_obj == null) {
                        v = "null";
                    } else {
                        v = v_obj.toString();
                    }


                    // If the key or value is multiple lines, format them nicely!
                    String value[] = LINE_SPLIT.split(v);
                    int total_lines = Math.max(key.length, value.length);
                    for (int line_i = 0; line_i < total_lines; line_i++) {
                        String k_line = (line_i < key.length ? key[line_i] : "");
                        if (upper) k_line = k_line.toUpperCase();

                        String v_line = (line_i < value.length ? value[line_i] : "");

                        if (line_i == (key.length - 1) && (!first || (first && !v_line.isEmpty()))) {
                            if (!equalsDelimiter && !k_line.trim().isEmpty()) k_line += ":";
                        }

                        blocks[map_i].append(String.format(f, k_line, v_line));
                        if (need_divider) max_value_size = Math.max(max_value_size, v_line.length());
                    } // FOR
                    if (v.endsWith("\n")) blocks[map_i].append("\n");
                }
                first = false;
            }
        } // FOR

        // Put it all together!
        int total_width = Math.max(max_title_size, (max_key_size + max_value_size + delimiter.length())) + 1;
        String dividing_line = (need_divider ? repeat("-", total_width) : "");
        StringBuilder sb;
        if (maps.length == 1) {
            sb = blocks[0];
        } else {
            sb = new StringBuilder();
            for (int i = 0; i < maps.length; i++) {
                if (blocks[i].length() == 0) continue;
                if (i != 0 && maps[i].size() > 0) sb.append(dividing_line).append("\n");
                sb.append(blocks[i]);
            } // FOR
        }
        return (box ? StringUtil.box(sb.toString()) :
                (border_top ? dividing_line + "\n" : "") + sb.toString() + (border_bottom ? dividing_line : ""));
    }

    /**
     * Returns the given string repeated the given # of times
     */
    private static String repeat(String str, int size) {
        // We cache the last call in case they are making repeated calls for the same thing
        if (CACHE_REPEAT_STR != null &&
                CACHE_REPEAT_STR.equals(str) &&
                CACHE_REPEAT_SIZE.equals(size)) {
            return (CACHE_REPEAT_RESULT);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) sb.append(str);
        CACHE_REPEAT_RESULT = sb.toString();
        CACHE_REPEAT_STR = str;
        CACHE_REPEAT_SIZE = size;
        return (CACHE_REPEAT_RESULT);
    }

    /**
     * Make a box around some text. If str has multiple lines, then the box will be the length
     * of the longest string.
     */
    private static String box(String str) {
        return (StringUtil.box(str, "*", null));
    }


    /**
     * Create a box around some text
     */
    private static String box(String str, String mark, Integer max_len) {
        String lines[] = LINE_SPLIT.split(str);
        if (lines.length == 0) return "";

        if (max_len == null) {
            for (String line : lines) {
                if (max_len == null || line.length() > max_len) max_len = line.length();
            } // FOR
        }

        final String top_line = StringUtil.repeat(mark, max_len + 4); // padding
        final String f = "%s %-" + max_len + "s %s\n";

        StringBuilder sb = new StringBuilder();
        sb.append(top_line).append("\n");
        for (String line : lines) {
            sb.append(String.format(f, mark, line, mark));
        } // FOR
        sb.append(top_line);

        return (sb.toString());
    }

    /**
     * Abbreviate the given string. The last three chars will be periods
     */
    private static String abbrv(String str, int max) {
        return (abbrv(str, max, true));
    }

    /**
     * Abbreviate the given string. If dots, then the last three chars will be periods
     */
    private static String abbrv(String str, int max, boolean dots) {
        String firstString = str.split("\n")[0];
        int len = firstString.length();
        String ret;
        if (len > max) {
            ret = (dots ? firstString.substring(0, max - 3) + "..." : firstString.substring(0, max));
        } else {
            ret = firstString;
        }
        return ret;
    }

    /**
     * Python join()
     */
    public static <T> String join(String delimiter, T... items) {
        return (join(delimiter, Arrays.asList(items)));
    }

    public static <T> String join(String delimiter, final Iterator<T> items) {
        return (join("", delimiter, CollectionUtil.iterable(items)));
    }

    /**
     * Python join()
     */
    public static String join(String delimiter, Iterable<?> items) {
        return (join("", delimiter, items));
    }

    /**
     * Python join() with optional prefix
     */
    public static String join(String prefix, String delimiter, Iterable<?> items) {
        if (items == null) return ("");
        if (prefix == null) prefix = "";

        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (Object x : items) {
            if (!prefix.isEmpty()) sb.append(prefix);
            sb.append(x != null ? x.toString() : x).append(delimiter);
            i++;
        }
        if (i == 0) return "";
        sb.delete(sb.length() - delimiter.length(), sb.length());

        return sb.toString();
    }
}
