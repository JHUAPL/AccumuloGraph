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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;



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
  public Iterator<Vertex> vertices(Direction direction) {

    // The vertex information needs to be loaded.
    if (inVertex == null || outVertex == null || label == null) {
      log.debug("Loading information for edge: "+this);
      globals.getEdgeWrapper().loadEndpointsAndLabel(this);
    }

    List<Vertex> verts = new ArrayList<Vertex>(2);
    if(Direction.IN.equals(direction) || Direction.BOTH.equals(direction)){
      verts.add(inVertex);
    }
    if(Direction.OUT.equals(direction) || Direction.BOTH.equals(direction)){
      verts.add(outVertex);
    }
    return verts.iterator();
  }



  @Override
  public void remove() {
    // Remove from named indexes.
    super.removeElementFromNamedIndexes();

    // If edge was removed already, forget it.
    // This may happen due to self-loops...
    if (!globals.getEdgeWrapper().elementExists(id)) {
      return;
    }

    // Remove properties from key/value indexes.
    Map<String, Object> props = globals.getEdgeWrapper()
        .readAllProperties(this);

    for (Entry<String,Object> ents : props.entrySet()) {
      globals.getEdgeKeyIndexWrapper().removePropertyFromIndex(this,
          ents.getKey(), ents.getValue());
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
    return StringFactory.edgeString(this);
  }

  @Override
  public String label() {
    // TODO less special treatment for "LABEL" property...
    if (label != null) {
      return label;
    }return "";
   // return getProperty(StringFactory.LABEL);
  }



  @Override
  public <V> Iterator<Property<V>> properties(String... propertyKeys) {
    // TODO Auto-generated method stub
    return null;
  }

}
