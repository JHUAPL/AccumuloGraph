/* Copyright 2014 The Johns Hopkins University Applied Physics Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.jhuapl.tinkerpop.mutator.property;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;

/**
 * TODO
 */
public final class PropertyUtils {

  private PropertyUtils() {
  }

  /**
   * Parse raw Accumulo entries into a property map.
   * If there are no entries, return null.
   * @param entries
   * @return
   */
  public static Map<String, Object> parseProperties(Iterable<Entry<Key, Value>> entries) {
    Map<String, Object> props = null;

    for (Entry<Key, Value> entry : entries) {
      if (props == null) {
        props = new HashMap<String, Object>();
      }

      Key key = entry.getKey();

      if (!isExistenceKey(key)) {
        String attr = key.getColumnFamily().toString();
        Object value = AccumuloByteSerializer.deserialize(entry.getValue().get());
        props.put(attr, value);
      }
    }

    return props;
  }

  /**
   * Test whether the given Accumulo key represents an
   * element's existence (i.e. not a property).
   * @param key
   * @return
   */
  private static boolean isExistenceKey(Key key) {
    return AccumuloGraph.TLABEL.equals(key.getColumnFamily()) &&
        AccumuloGraph.TEXISTS.equals(key.getColumnQualifier());
  }
}
