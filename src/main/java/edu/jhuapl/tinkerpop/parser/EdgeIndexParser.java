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

import edu.jhuapl.tinkerpop.AccumuloEdge;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * Edge-specific index parser.
 */
public class EdgeIndexParser extends ElementIndexParser<AccumuloEdge> {

  public EdgeIndexParser(GlobalInstances globals) {
    super(globals);
  }

  @Override
  protected AccumuloEdge instantiate(String id) {
    return new AccumuloEdge(globals, id);
  }
}
