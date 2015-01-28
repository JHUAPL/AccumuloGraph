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

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloEdge;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.Constants;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * TODO
 */
public class EdgeParser extends ElementParser<AccumuloEdge> {

  public EdgeParser(GlobalInstances globals) {
    super(globals);
  }

  @Override
  public AccumuloEdge parse(String id, Iterable<Entry<Key,Value>> entries) {
    AccumuloEdge edge = makeEdge(id, entries);
    setInMemoryProperties(edge, entries);
    return edge;
  }

  /**
   * Make and return an edge object. If the entries
   * contain label/endpoint information, set those too.
   * @param id
   * @param entries
   * @return
   */
  private AccumuloEdge makeEdge(String id, Iterable<Entry<Key,Value>> entries) {
    for (Entry<Key, Value> entry : entries) {
      String cf = entry.getKey().getColumnFamily().toString();
      if (Constants.LABEL.equals(cf)) {
        String cq = entry.getKey().getColumnQualifier().toString();
        String[] parts = cq.split(Constants.ID_DELIM);
        String inVertexId = parts[0];
        String outVertexId = parts[1];
        String label = AccumuloByteSerializer.deserialize(entry.getValue().get());
        return new AccumuloEdge(globals, id,
            new AccumuloVertex(globals, inVertexId),
            new AccumuloVertex(globals, outVertexId), label);
      }
    }

    // This should not happen.
    throw new AccumuloGraphException("Unable to parse edge from entries");
  }
}
