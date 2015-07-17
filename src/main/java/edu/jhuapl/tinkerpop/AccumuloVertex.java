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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.tinkerpop.blueprints.util.ExceptionFactory;


/**
 * TODO
 */
public class AccumuloVertex extends AccumuloElement implements Vertex {

  public AccumuloVertex(GlobalInstances globals, String id) {
    super(globals, id, Vertex.class);
  }

  //TODO
  public <V> VertexProperty<V> property(String key, V value){
    return null;
  }



 // @Override
  public Edge addEdge(String label, Vertex inVertex) {
    return addEdge(null, label, inVertex);
  }

  /**
   * Add an edge as with {@link #addEdge(String, Vertex)},
   * but with a specified edge id.
   * @param id
   * @param label
   * @param inVertex
   * @return
   */
  public Edge addEdge(Object id, String label, Vertex inVertex) {
    if (label == null) {
      throw ExceptionFactory.edgeLabelCanNotBeNull();
    }
    if (id == null) {
      id = AccumuloGraphUtils.generateId();
    }

    String myID = id.toString();

    AccumuloEdge edge = new AccumuloEdge(globals, myID, inVertex, this, label);

    // TODO we arent suppose to make sure the given edge ID doesn't already
    // exist?

    globals.getEdgeWrapper().writeEdge(edge);
    globals.getVertexWrapper().writeEdgeEndpoints(edge);

    globals.checkedFlush();

    globals.getCaches().cache(edge, Edge.class);

    return edge;
  }

  @Override
  public void remove() {
    globals.getCaches().remove(id(), Vertex.class);

    super.removeElementFromNamedIndexes();

    // Throw exception if the element does not exist.
    if (!globals.getVertexWrapper().elementExists(id)) {
      throw ExceptionFactory.vertexWithIdDoesNotExist(id());
    }

    // Remove properties from key/value indexes.
    Map<String, Object> props = globals.getVertexWrapper()
        .readAllProperties(this);

    for (Entry<String,Object> ent : props.entrySet()) {
      globals.getVertexKeyIndexWrapper().removePropertyFromIndex(this,
          ent.getKey(), ent.getValue());
    }

    // Remove edges incident to this vertex.
    Iterator<Edge> iter = edges(Direction.BOTH);
    while (iter.hasNext()) {
      iter.next().remove();
    }
    //iter.close();

    globals.checkedFlush();

    // Get rid of the vertex.
    globals.getVertexWrapper().deleteVertex(this);
    globals.checkedFlush();
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }

  @Override
  public String label() {
    // TODO Auto-generated method stub
    return null;
  }



  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V> VertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    return globals.getVertexWrapper().getEdges(this, direction, edgeLabels).iterator();
  }

  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    return globals.getVertexWrapper().getVertices(this, direction, edgeLabels).iterator();
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    // TODO Auto-generated method stub
    return null;
  }

}
