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
package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import edu.jhuapl.tinkerpop.AccumuloGraph.Type;

public class AccumuloEdge extends AccumuloElement implements Edge {

  String label;
  String inId;
  String outId;
  Vertex inVertex;
  Vertex outVertex;

  AccumuloEdge(AccumuloGraph parent, String id) {
    this(parent, id, null);
  }

  AccumuloEdge(AccumuloGraph parent, String id, String label) {
    this(parent, id, label, (Vertex) null, (Vertex) null);
  }

  AccumuloEdge(AccumuloGraph parent, String id, String label, Vertex inVertex, Vertex outVertex) {
    super(parent, id, Type.Edge);
    this.label = label;
    this.inVertex = inVertex;
    this.outVertex = outVertex;
  }

  AccumuloEdge(AccumuloGraph parent, String id, String label, String inVertex, String outVertex) {
    super(parent, id, Type.Edge);
    this.label = label;
    this.inId = inVertex;
    this.outId = outVertex;
  }

  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    switch (direction) {
      case IN:
        if (inVertex == null) {
          if (inId == null) {
            inVertex = parent.getEdgeVertex(id, direction);
            inId = inVertex.getId().toString();
          } else {
            inVertex = parent.getVertex(inId);
          }
        }
        return inVertex;
      case OUT:
        if (outVertex == null) {
          if (outId == null) {
            outVertex = parent.getEdgeVertex(id, direction);
            outId = outVertex.getId().toString();
          } else {
            outVertex = parent.getVertex(outId);
          }
        }
        return outVertex;
      case BOTH:
        throw ExceptionFactory.bothIsNotSupported();
      default:
        throw new RuntimeException("Unexpected direction: " + direction);
    }
  }

  public String getLabel() {
    // TODO less special treatment for "LABEL" property...
    if (label != null) {
      return label;
    }
    return getProperty(StringFactory.LABEL);
  }

  public void remove() {
    parent.removeEdge(this);
  }

  public String getInId() {
    return inId;
  }

  public String getOutId() {
    return outId;
  }

  public String toString() {
    return "[" + getId() + ":" + getVertex(Direction.OUT) + " -> " + getLabel() + " -> " + getVertex(Direction.IN) + "]";
  }
}
