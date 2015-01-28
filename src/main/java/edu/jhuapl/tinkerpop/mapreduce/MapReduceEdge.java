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
package edu.jhuapl.tinkerpop.mapreduce;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.jhuapl.tinkerpop.AccumuloGraph;

public class MapReduceEdge extends MapReduceElement implements Edge {

  String sourceId;
  String label;
  String destinationId;

  MapReduceEdge(AccumuloGraph parent) {
    super(parent);
  }

  void setSourceId(String id) {
    sourceId = id;
  }

  void setDestId(String id) {
    destinationId = id;
  }

  MapReduceEdge(AccumuloGraph parent, String id, String src, String label, String dest) {
    super(parent);
    prepareId(id);
    this.sourceId = src;
    this.label = label;
    this.destinationId = dest;
  }

  @Override
  public String getLabel() {
    return label;
  }

  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    return parent.getVertex(getVertexId(direction));
  }

  public String getVertexId(Direction direction) throws IllegalArgumentException {
    switch (direction) {
      case IN:
        return destinationId;
      case OUT:
        return sourceId;
      case BOTH:
      default:
        throw ExceptionFactory.bothIsNotSupported();
    }
  }

  public void setLabel(String label) {
    this.label = label;
  }

}
