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

import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * TODO
 */
public class VertexParser extends ElementParser<AccumuloVertex> {

  public VertexParser(GlobalInstances globals) {
    super(globals);
  }

  @Override
  public AccumuloVertex parse(String id, Iterable<Entry<Key,Value>> entries) {
    AccumuloVertex vertex = new AccumuloVertex(globals, id);
    setInMemoryProperties(vertex, entries);
    return vertex;
  }
}
