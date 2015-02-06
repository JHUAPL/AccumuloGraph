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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.jhuapl.tinkerpop.cache.ElementCaches;

/**
 * This is an implementation of the TinkerPop Blueprints 2.6 API using
 * Apache Accumulo as the backend. This combines the many benefits and flexibility
 * of Blueprints with the scalability and performance of Accumulo.
 * 
 * <p/>In addition to the basic Blueprints functionality, we provide a number of
 * enhanced features, including:
 * <ol>
 * <li>Indexing implementations via IndexableGraph and KeyIndexableGraph</li>
 * <li>Support for mock, mini, and distributed instances of Accumulo</li>
 * <li>Numerous performance tweaks and configuration parameters</li>
 * <li>Support for high speed ingest</li>
 * <li>Hadoop integration</li>
 * </ol>
 */
public class AccumuloGraph implements Graph, KeyIndexableGraph, IndexableGraph {

  private final GlobalInstances globals;

  /**
   * Factory method for {@link GraphFactory}.
   */
  public static AccumuloGraph open(Configuration properties) throws AccumuloException {
    return new AccumuloGraph(properties);
  }

  /**
   * Instantiate from a generic {@link Configuration} populated
   * with appropriate AccumuloGraph parameters.
   * @param cfg
   */
  public AccumuloGraph(Configuration cfg) {
    this(new AccumuloGraphConfiguration(cfg));
  }

  /**
   * Main constructor.
   * @param config
   */
  public AccumuloGraph(AccumuloGraphConfiguration config) {
    config.validate();

    AccumuloGraphUtils.handleCreateAndClear(config);

    try {
      globals = new GlobalInstances(config, config.getConnector()
          .createMultiTableBatchWriter(config.getBatchWriterConfig()),
          new ElementCaches(config));
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  /**
   * This is at package level for the {@link AutoIndexTest} unit test
   * and probably should disappear.
   * @return
   */
  GlobalInstances getGlobals() {
    return globals;
  }

  @Override
  public Features getFeatures() {
    return AccumuloFeatures.get();
  }

  @Override
  public Vertex addVertex(Object id) {
    if (id == null) {
      id = AccumuloGraphUtils.generateId();
    }

    String idStr = id.toString();

    Vertex vert = null;
    if (!globals.getConfig().getSkipExistenceChecks()) {
      vert = getVertex(idStr);
      if (vert != null) {
        throw ExceptionFactory.vertexWithIdAlreadyExists(idStr);
      }
    }

    vert = new AccumuloVertex(globals, idStr);

    globals.getVertexWrapper().writeVertex(vert);
    globals.checkedFlush();

    globals.getCaches().cache(vert, Vertex.class);

    return vert;
  }

  @Override
  public Vertex getVertex(Object id) {
    if (id == null) {
      throw ExceptionFactory.vertexIdCanNotBeNull();
    }
    String myID = id.toString();

    Vertex vertex = globals.getCaches().retrieve(myID, Vertex.class);
    if (vertex != null) {
      return vertex;
    }

    vertex = new AccumuloVertex(globals, myID);
    if (!globals.getConfig().getSkipExistenceChecks()) {
      // In addition to just an "existence" check, we will also load
      // any "preloaded" properties now, which saves us a round-trip
      // to Accumulo later.
      String[] preload = globals.getConfig().getPreloadedProperties();
      if (preload == null) {
        preload = new String[]{};
      }

      Map<String, Object> props = globals.getVertexWrapper().readProperties(vertex, preload);
      if (props == null) {
        return null;
      }

      for (Entry<String, Object> ents : props.entrySet()) {
        ((AccumuloElement) vertex).setPropertyInMemory(ents.getKey(), ents.getValue());
      }
    }

    globals.getCaches().cache(vertex, Vertex.class);

    return vertex;
  }

  @Override
  public void removeVertex(Vertex vertex) {
    vertex.remove();
  }

  @Override
  public Iterable<Vertex> getVertices() {
    return globals.getVertexWrapper().getVertices();
  }

  /**
   * Retrieve vertices with ids within the given range,
   * inclusive. The range is calculated using the string
   * representations of the given ids. If fromId or
   * toId is null, use negative infinity and positive
   * infinity, respectively.
   * <p/>Note: This does not use indexes.
   * @param fromId
   * @param toId
   * @return
   */
  public Iterable<Vertex> getVerticesInRange(Object fromId, Object toId) {
    return globals.getVertexWrapper().getVerticesInRange(fromId, toId);
  }

  @Override
  public Iterable<Vertex> getVertices(String key, Object value) {
    AccumuloGraphUtils.validateProperty(key, value);
    if (globals.getConfig().getAutoIndex() || getIndexedKeys(Vertex.class).contains(key)) {
      return globals.getVertexKeyIndexWrapper().getVertices(key, value);
    } else {
      return globals.getVertexWrapper().getVertices(key, value);
    }
  }

  @Override
  public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
    return ((AccumuloVertex) outVertex).addEdge(id, label, inVertex);
  }

  @Override
  public Edge getEdge(Object id) {
    if (id == null) {
      throw ExceptionFactory.edgeIdCanNotBeNull();
    }
    String idStr = id.toString();

    Edge edge = globals.getCaches().retrieve(idStr, Edge.class);
    if (edge != null) {
      return edge;
    }

    edge = new AccumuloEdge(globals, idStr);

    if (!globals.getConfig().getSkipExistenceChecks()) {
      // In addition to just an "existence" check, we will also load
      // any "preloaded" properties now, which saves us a round-trip
      // to Accumulo later.
      String[] preload = globals.getConfig().getPreloadedProperties();
      if (preload == null) {
        preload = new String[]{};
      }

      Map<String, Object> props = globals.getEdgeWrapper()
          .readProperties(edge, preload);
      // This will be null if the element does not exist,
      // in which case return null.
      if (props == null) {
        return null;
      }

      for (Entry<String, Object> ents : props.entrySet()) {
        ((AccumuloElement) edge).setPropertyInMemory(ents.getKey(), ents.getValue());
      }
    }

    globals.getCaches().cache(edge, Edge.class);

    return edge;
  }

  @Override
  public void removeEdge(Edge edge) {
    edge.remove();
  }

  @Override
  public Iterable<Edge> getEdges() {
    return globals.getEdgeWrapper().getEdges();
  }

  @Override
  public Iterable<Edge> getEdges(String key, Object value) {
    AccumuloGraphUtils.nullCheckProperty(key, value);
    if (key.equalsIgnoreCase("label")) {
      key = Constants.LABEL;
    }

    if (globals.getConfig().getAutoIndex() || getIndexedKeys(Edge.class).contains(key)) {
      return globals.getEdgeKeyIndexWrapper().getEdges(key, value);
    } else {
      return globals.getEdgeWrapper().getEdges(key, value);
    }
  }

  // TODO Eventually
  @Override
  public GraphQuery query() {
    return new DefaultGraphQuery(this);
  }

  @Override
  public void shutdown() {
    try {
      globals.getMtbw().close();
      globals.getVertexWrapper().close();
      globals.getEdgeWrapper().close();
    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
    globals.getCaches().clear(Vertex.class);
    globals.getCaches().clear(Edge.class);
  }

  @Override
  public String toString() {
    return AccumuloGraphConfiguration.ACCUMULO_GRAPH_CLASS.getSimpleName().toLowerCase();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <T extends Element> Index<T> createIndex(String indexName,
      Class<T> indexClass, Parameter... indexParameters) {
    if (indexClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }
    else if (globals.getConfig().getIndexableGraphDisabled()) {
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");
    }

    for (Index<?> index : globals.getNamedIndexListWrapper().getIndices()) {
      if (index.getIndexName().equals(indexName)) {
        throw ExceptionFactory.indexAlreadyExists(indexName);
      }
    }

    return globals.getNamedIndexListWrapper().createIndex(indexName, indexClass);
  }

  @Override
  public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
    if (indexClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }
    else if (globals.getConfig().getIndexableGraphDisabled()) {
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");
    }

    return globals.getNamedIndexListWrapper().getIndex(indexName, indexClass);
  }

  @Override
  public Iterable<Index<? extends Element>> getIndices() {
    if (globals.getConfig().getIndexableGraphDisabled()) {
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");
    }
    return globals.getNamedIndexListWrapper().getIndices();
  }

  @Override
  public void dropIndex(String indexName) {
    if (globals.getConfig().getIndexableGraphDisabled())
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");

    for (Index<? extends Element> index : getIndices()) {
      if (index.getIndexName().equals(indexName)) {
        globals.getNamedIndexListWrapper().clearIndexNameEntry(indexName, index.getIndexClass());

        try {
          globals.getConfig().getConnector().tableOperations().delete(globals.getConfig()
              .getNamedIndexTableName(indexName));
        } catch (Exception e) {
          throw new AccumuloGraphException(e);
        }

        return;
      }
    }

    throw new AccumuloGraphException("Index does not exist: "+indexName);
  }

  @Override
  public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
    // TODO Move below to somewhere appropriate.
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }

    globals.getIndexedKeysListWrapper().clearKeyMetadataEntry(key, elementClass);

    String table = null;
    if (elementClass.equals(Vertex.class)) {
      table = globals.getConfig().getVertexKeyIndexTableName();
    } else {
      table = globals.getConfig().getEdgeKeyIndexTableName();
    }
    BatchDeleter bd = null;
    try {
      bd = globals.getConfig().getConnector().createBatchDeleter(table, globals.getConfig().getAuthorizations(), globals.getConfig().getMaxWriteThreads(), globals.getConfig().getBatchWriterConfig());
      bd.setRanges(Collections.singleton(new Range()));
      bd.fetchColumnFamily(new Text(key));
      bd.delete();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (bd != null)
        bd.close();
    }
    globals.checkedFlush();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <T extends Element> void createKeyIndex(String key,
      Class<T> elementClass, Parameter... indexParameters) {
    // TODO Move below to somewhere appropriate.
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }

    // Add key to indexed keys list.
    globals.getIndexedKeysListWrapper().writeKeyMetadataEntry(key, elementClass);
    globals.checkedFlush();

    // Reindex graph.
    globals.getKeyIndexTableWrapper(elementClass).rebuildIndex(key, elementClass);
    globals.getVertexKeyIndexWrapper().dump();
    globals.checkedFlush();
  }

  @Override
  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    return globals.getIndexedKeysListWrapper().getIndexedKeys(elementClass);
  }

  /**
   * Clear out this graph. This drops and recreates the backing tables.
   */
  public void clear() {
    shutdown();

    try {
      TableOperations tableOps = globals.getConfig()
          .getConnector().tableOperations();
      for (Index<? extends Element> index : getIndices()) {
        tableOps.delete(((AccumuloIndex<? extends Element>)
            index).getTableName());
      }

      for (String table : globals.getConfig().getTableNames()) {
        if (tableOps.exists(table)) {
          tableOps.delete(table);
          tableOps.create(table);

          SortedSet<Text> splits = globals.getConfig().getSplits();
          if (splits != null) {
            tableOps.addSplits(table, splits);
          }
        }
      }
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  public boolean isEmpty() {
    try {
      TableOperations tableOps = globals.getConfig().getConnector().tableOperations();
      for (String table : globals.getConfig().getTableNames()) {
        if (tableOps.getMaxRow(table, globals.getConfig().getAuthorizations(),
            null, true, null, true) != null) {
          return false;
        }
      }
      return true;

    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }
}
