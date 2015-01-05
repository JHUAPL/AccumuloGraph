/******************************************************************************
 *                              COPYRIGHT NOTICE                              *
 * Copyright (c) 2014 The Johns Hopkins University/Applied Physics Laboratory *
 *                            All rights reserved.                            *
 *                                                                            *
 * This material may only be used, modified, or reproduced by or for the      *
 * U.S. Government pursuant to the license rights granted under FAR clause    *
 * 52.227-14 or DFARS clauses 252.227-7013/7014.                              *
 *                                                                            *
 * For any other permissions, please contact the Legal Office at JHU/APL.     *
 ******************************************************************************/
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
