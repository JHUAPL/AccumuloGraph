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

import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.cache.ElementCaches;
import edu.jhuapl.tinkerpop.tables.EdgeIndexTableWrapper;
import edu.jhuapl.tinkerpop.tables.EdgeTableWrapper;
import edu.jhuapl.tinkerpop.tables.ElementTableWrapper;
import edu.jhuapl.tinkerpop.tables.IndexNameTableWrapper;
import edu.jhuapl.tinkerpop.tables.IndexedKeysTableWrapper;
import edu.jhuapl.tinkerpop.tables.VertexIndexTableWrapper;
import edu.jhuapl.tinkerpop.tables.VertexTableWrapper;

/**
 * Internal class gathering together instances of
 * objects needed for various AccumuloGraph components.
 */
public class GlobalInstances {

  private final AccumuloGraph graph;
  private final AccumuloGraphConfiguration config;
  private final MultiTableBatchWriter mtbw;
  private VertexTableWrapper vertexWrapper;
  private EdgeTableWrapper edgeWrapper;
  private VertexIndexTableWrapper vertexIndexWrapper;
  private EdgeIndexTableWrapper edgeIndexWrapper;
  private IndexedKeysTableWrapper keyMetadataWrapper;
  private IndexNameTableWrapper indexMetadataWrapper;
  private final ElementCaches caches;

  public GlobalInstances(AccumuloGraph graph,
      AccumuloGraphConfiguration config, MultiTableBatchWriter mtbw,
      ElementCaches caches) {
    this.graph = graph;
    this.config = config;
    this.mtbw = mtbw;
    this.caches = caches;
  }

  public AccumuloGraph getGraph() {
    return graph;
  }

  public AccumuloGraphConfiguration getConfig() {
    return config;
  }

  public MultiTableBatchWriter getMtbw() {
    return mtbw;
  }

  public VertexTableWrapper getVertexWrapper() {
    return vertexWrapper;
  }

  public EdgeTableWrapper getEdgeWrapper() {
    return edgeWrapper;
  }

  public VertexIndexTableWrapper getVertexIndexWrapper() {
    return vertexIndexWrapper;
  }

  public EdgeIndexTableWrapper getEdgeIndexWrapper() {
    return edgeIndexWrapper;
  }

  public IndexedKeysTableWrapper getKeyMetadataWrapper() {
    return keyMetadataWrapper;
  }

  public IndexNameTableWrapper getIndexMetadataWrapper() {
    return indexMetadataWrapper;
  }

  public <T extends Element> ElementTableWrapper getElementWrapper(Class<T> clazz) {
    if (Vertex.class.equals(clazz)) {
      return vertexWrapper;
    } else if (Edge.class.equals(clazz)) {
      return edgeWrapper;
    } else {
      throw new AccumuloGraphException("Unrecognized class: "+clazz);
    }
  }

  public ElementCaches getCaches() {
    return caches;
  }

  /**
   * Flush the writer, if autoflush is enabled.
   */
  public void checkedFlush() {
    if (config.getAutoFlush()) {
      try {
        mtbw.flush();
      } catch (MutationsRejectedException e) {
        throw new AccumuloGraphException(e);
      }
    }
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param wrapper
   */
  @Deprecated
  public void setVertexWrapper(VertexTableWrapper wrapper) {
    this.vertexWrapper = wrapper;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param wrapper
   */
  @Deprecated
  public void setEdgeWrapper(EdgeTableWrapper wrapper) {
    this.edgeWrapper = wrapper;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param wrapper
   */
  @Deprecated
  public void setVertexIndexWrapper(VertexIndexTableWrapper wrapper) {
    this.vertexIndexWrapper = wrapper;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param wrapper
   */
  @Deprecated
  public void setEdgeIndexWrapper(EdgeIndexTableWrapper wrapper) {
    this.edgeIndexWrapper = wrapper;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param wrapper
   */
  @Deprecated
  public void setKeyMetadataWrapper(IndexedKeysTableWrapper wrapper) {
    this.keyMetadataWrapper = wrapper;
  }

  /**
   * TODO: Refactor these away when the {@link #graph} member is gone.
   * @param wrapper
   */
  @Deprecated
  public void setIndexMetadataWrapper(IndexNameTableWrapper indexMetadataWrapper) {
    this.indexMetadataWrapper = indexMetadataWrapper;
  }
}
