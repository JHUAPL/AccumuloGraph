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

import java.util.Map;

import org.apache.log4j.Logger;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * TODO
 */
public class AccumuloEdge extends AccumuloElement implements Edge {

  private static final Logger log = Logger.getLogger(AccumuloEdge.class);

  private String label;
  private Vertex inVertex;
  private Vertex outVertex;

  public AccumuloEdge(GlobalInstances globals, String id) {
    this(globals, id, null, null, null);
  }

  public AccumuloEdge(GlobalInstances globals, String id,
      Vertex inVertex, Vertex outVertex, String label) {
    super(globals, id, Edge.class);
    this.label = label;
    this.inVertex = inVertex;
    this.outVertex = outVertex;
  }

  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    if (!Direction.IN.equals(direction) && !Direction.OUT.equals(direction)) {
      throw new IllegalArgumentException("Invalid direction: "+direction);
    }

    // The vertex information needs to be loaded.
    if (inVertex == null || outVertex == null || label == null) {
      log.debug("Loading information for edge: "+this);
      globals.getEdgeWrapper().loadEndpointsAndLabel(this);
    }

    return Direction.IN.equals(direction) ? inVertex : outVertex;
  }

  @Override
  public String getLabel() {
    // TODO less special treatment for "LABEL" property...
    if (label != null) {
      return label;
    }
    return getProperty(StringFactory.LABEL);
  }

  @Override
  public void remove() {
    // Remove from named indexes.
    super.removeElementFromNamedIndexes();

    // Remove from key/value indexes.
    Map<String, Object> props = globals.getEdgeWrapper()
        .readProperties(this);
    for (String key : props.keySet()) {
      globals.getEdgeKeyIndexWrapper().removePropertyFromIndex(this,
          key, props.get(key));
    }

    // Get rid of the endpoints and edge themselves.
    globals.getVertexWrapper().deleteEdgeEndpoints(this);
    globals.getEdgeWrapper().deleteEdge(this);

    // Remove element from cache.
    globals.getCaches().remove(id, Edge.class);

    globals.checkedFlush();
  }

  public void setVertices(AccumuloVertex inVertex, AccumuloVertex outVertex) {
    this.inVertex = inVertex;
    this.outVertex = outVertex;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return "[" + getId() + ":" + inVertex + " -> " + label + " -> " + outVertex + "]";
  }
}
