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
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * TODO
 */
public class AccumuloVertex extends AccumuloElement implements Vertex {

  public AccumuloVertex(GlobalInstances globals, String id) {
    super(globals, id, Vertex.class);
  }

  @Override
  public Iterable<Edge> getEdges(Direction direction, String... labels) {
    return globals.getVertexWrapper().getEdges(this, direction, labels);
  }

  @Override
  public Iterable<Vertex> getVertices(Direction direction, String... labels) {
    return globals.getVertexWrapper().getVertices(this, direction, labels);
  }

  @Override
  public VertexQuery query() {
    return new DefaultVertexQuery(this);
  }

  @Override
  public Edge addEdge(String label, Vertex inVertex) {
    return globals.getGraph().addEdge(null, this, inVertex, label);
  }

  @Override
  public void remove() {
    globals.getCaches().remove(getId(), Vertex.class);

    super.removeElementFromNamedIndexes();

    // Throw exception if the element does not exist.
    if (!globals.getVertexWrapper().elementExists(id)) {
      throw ExceptionFactory.vertexWithIdDoesNotExist(getId());
    }

    // Remove properties from key/value indexes.
    Set<String> indexedKeys = globals.getIndexedKeysListWrapper()
        .getIndexedKeys(Vertex.class);

    Map<String, Object> props = globals.getVertexWrapper()
        .readProperties(this, indexedKeys.toArray(ArrayUtils.EMPTY_STRING_ARRAY));

    for (String key : props.keySet()) {
      globals.getVertexKeyIndexWrapper().removePropertyFromIndex(this,
          key, props.get(key));
    }

    // Remove edges incident to this vertex.
    for (Edge edge : getEdges(Direction.BOTH)) {
      edge.remove();
    }

    globals.checkedFlush();

    // Get rid of the vertex.
    globals.getVertexWrapper().deleteVertex(this);
    globals.checkedFlush();
  }

  @Override
  public String toString() {
    return "[" + getId() + "]";
  }

}
