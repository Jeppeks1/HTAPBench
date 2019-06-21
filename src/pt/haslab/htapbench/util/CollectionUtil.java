
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
 * @author pavlo
 */
/**
 *
 * @author pavlo
 *
 */
package pt.haslab.htapbench.util;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.commons.lang.NotImplementedException;

import java.util.*;

/**
 *
 */
public abstract class CollectionUtil {

    private static final Random RANDOM = new Random();

    /**
     * Put all the values of an Iterator into a List
     */
    public static <T> List<T> list(Iterator<T> it) {
        List<T> list = new ArrayList<T>();
        CollectionUtil.addAll(list, it);
        return (list);
    }

    /**
     * Put all of the values of an Enumeration into a new List
     */
    public static <T> List<T> list(Enumeration<T> e) {
        return (list(iterable(e)));
    }

    /**
     * Put all of the values of an Iterable into a new List
     */
    public static <T> List<T> list(Iterable<T> it) {
        return (list(it.iterator()));
    }

    /**
     * Add all of the items from the Iterator into the given collection
     */
    private static <T> Collection<T> addAll(Collection<T> data, Iterator<T> items) {
        while (items.hasNext()) {
            data.add(items.next());
        } // WHILE
        return (data);
    }

    /**
     * Return a random value from the given Collection
     */
    public static <T> T random(Collection<T> items) {
        return (CollectionUtil.random(items, RANDOM));
    }

    /**
     * Return a random value from the given Collection
     */
    public static <T> T random(Collection<T> items, Random rand) {
        int idx = rand.nextInt(items.size());
        return (CollectionUtil.get(items, idx));
    }

    /**
     * Return a random value from the given Iterable
     */
    public static <T> T random(Iterable<T> it) {
        return (CollectionUtil.random(it, RANDOM));
    }

    /**
     * Return a random value from the given Iterable
     */
    public static <T> T random(Iterable<T> it, Random rand) {
        List<T> list = new ArrayList<T>();

        for (T t : it) {
            list.add(t);
        }

        return (CollectionUtil.random(list, rand));
    }


    /**
     * Add all the items in the array to a Collection
     */
    static <T> Collection<T> addAll(Collection<T> data, T... items) {
        data.addAll(Arrays.asList((T[]) items));
        return (data);
    }

    /**
     * Return the first item in a Iterable
     */
    public static <T> T first(Iterable<T> items) {
        return (CollectionUtil.get(items, 0));
    }

    /**
     * Return the first item in a Iterator
     */
    public static <T> T first(Iterator<T> items) {
        return (items.hasNext() ? items.next() : null);
    }

    /**
     * Returns the first item in an Enumeration
     */
    public static <T> T first(Enumeration<T> items) {
        return (items.hasMoreElements() ? items.nextElement() : null);
    }

    /**
     * Return the ith element of a set. Super lame
     */
    public static <T> T get(Iterable<T> items, int idx) {
        if (items instanceof AbstractList<?>) {
            return ((AbstractList<T>) items).get(idx);
        } else if (items instanceof ListOrderedSet<?>) {
            return ((ListOrderedSet<T>) items).get(idx);
        }
        int ctr = 0;
        for (T t : items) {
            if (ctr++ == idx) return (t);
        }
        return (null);
    }


    /**
     * Return the last item in an array
     */
    public static <T> T last(T... items) {
        if (items != null && items.length > 0) {
            return (items[items.length - 1]);
        }
        return (null);
    }


    /**
     * Wrap an Iterable around an Iterator
     */
    static <T> Iterable<T> iterable(final Iterator<T> it) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return (it);
            }
        });
    }

    /**
     * Wrap an Iterable around an Enumeration
     */
    private static <T> Iterable<T> iterable(final Enumeration<T> e) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return (e.hasMoreElements());
                    }

                    @Override
                    public T next() {
                        return (e.nextElement());
                    }

                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        });
    }
}
