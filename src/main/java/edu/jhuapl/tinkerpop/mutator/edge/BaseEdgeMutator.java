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
package edu.jhuapl.tinkerpop.mutator.edge;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.mutator.Mutator;

public abstract class BaseEdgeMutator implements Mutator {

  protected final String id;
  protected final String outVertexId;
  protected final String inVertexId;
  protected final String label;

  public BaseEdgeMutator(Edge edge) {
    this(edge.getId().toString(),
        edge.getVertex(Direction.OUT).getId().toString(),
        edge.getVertex(Direction.IN).getId().toString(),
        edge.getLabel());
  }
  public BaseEdgeMutator(String id, String outVertexId, String inVertexId, String label) {
    this.id = id;
    this.outVertexId = outVertexId;
    this.inVertexId = inVertexId;
    this.label = label;
  }
}
