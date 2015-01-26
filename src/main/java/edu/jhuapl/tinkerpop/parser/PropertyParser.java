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
package edu.jhuapl.tinkerpop.parser;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.Constants;

/**
 * TODO
 */
public class PropertyParser implements EntryParser<Map<String, Object>> {

  @Override
  public Map<String,Object> parse(Iterable<Entry<Key,Value>> entries) {
    Map<String, Object> props = null;

    for (Entry<Key, Value> entry : entries) {
      if (props == null) {
        props = new HashMap<String, Object>();
      }

      Key key = entry.getKey();

      if (!isMetaKey(key)) {
        String attr = key.getColumnFamily().toString();
        Object value = AccumuloByteSerializer.deserialize(entry.getValue().get());
        props.put(attr, value);
      }
    }

    return props;
  }

  /**
   * Test whether the given Accumulo key represents a
   * metadata key (e.g. existence, edge endpoint, etc),
   * rather than a property.
   * @param key
   * @return
   */
  private static boolean isMetaKey(Key key) {
    String cf = key.getColumnFamily().toString();
    String cq = key.getColumnQualifier().toString();
    return (Constants.LABEL.equals(cf) &&
        Constants.EXISTS.equals(cq)) ||
        Constants.IN_EDGE.equals(cf) ||
        Constants.OUT_EDGE.equals(cf);
  }

}
