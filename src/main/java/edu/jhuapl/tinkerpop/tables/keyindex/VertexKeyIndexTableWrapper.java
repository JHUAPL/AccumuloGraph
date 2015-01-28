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
package edu.jhuapl.tinkerpop.tables.keyindex;

import java.util.Arrays;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.ScannerIterable;
import edu.jhuapl.tinkerpop.parser.VertexIndexParser;

/**
 * Wrapper around {@link Vertex} index table.
 */
public class VertexKeyIndexTableWrapper extends BaseKeyIndexTableWrapper {

  public VertexKeyIndexTableWrapper(GlobalInstances globals) {
    super(globals, Vertex.class, globals.getConfig()
        .getVertexKeyIndexTableName());
  }

  /**
   * Use the index to retrieve vertices with the
   * given key/value.
   * @param key
   * @param value
   */
  public Iterable<Vertex> getVertices(String key, Object value) {
    Scanner s = getScanner();

    Text row = new Text(AccumuloByteSerializer.serialize(value));
    s.setRange(Range.exact(row));
    s.fetchColumnFamily(new Text(key));

    final VertexIndexParser parser = new VertexIndexParser(globals);

    return new ScannerIterable<Vertex>(s) {

      @Override
      public Vertex next(PeekingIterator<Entry<Key, Value>> iterator) {
        Entry<Key, Value> entry = iterator.next();
        AccumuloVertex v = parser.parse(Arrays.asList(entry));

        // Check if we have it cached already, in which
        // case use the cached version.
        AccumuloVertex cached = (AccumuloVertex) globals.getCaches()
            .retrieve(v.getId(), Vertex.class);
        if (cached != null) {
          for (String key : v.getPropertyKeysInMemory()) {
            cached.setPropertyInMemory(key, v.getPropertyInMemory(key));
          }

          return cached;
        }

        // We don't have it, so cache the new one and return it.
        globals.getCaches().cache(v, Vertex.class);
        return v;
      }
    };
  }
}
