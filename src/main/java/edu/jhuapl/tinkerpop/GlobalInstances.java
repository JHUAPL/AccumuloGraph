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
import edu.jhuapl.tinkerpop.tables.IndexTableWrapper;
import edu.jhuapl.tinkerpop.tables.IndexedKeysTableWrapper;
import edu.jhuapl.tinkerpop.tables.VertexIndexTableWrapper;
import edu.jhuapl.tinkerpop.tables.VertexTableWrapper;

/**
 * Internal class gathering together instances of
 * objects needed for various AccumuloGraph components.
 */
public class GlobalInstances {

  /**
   * This member should eventually disappear - users
   * should be able to access everything through this class.
   */
  @Deprecated
  private final AccumuloGraph graph;

  private final AccumuloGraphConfiguration config;
  private final MultiTableBatchWriter mtbw;
  private final ElementCaches caches;

  public GlobalInstances(AccumuloGraph graph,
      AccumuloGraphConfiguration config, MultiTableBatchWriter mtbw,
      ElementCaches caches) {
    this.graph = graph;
    this.config = config;
    this.mtbw = mtbw;
    this.caches = caches;
  }

  @Deprecated
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
    return new VertexTableWrapper(this);
  }

  public EdgeTableWrapper getEdgeWrapper() {
    return new EdgeTableWrapper(this);
  }

  public VertexIndexTableWrapper getVertexIndexWrapper() {
    return new VertexIndexTableWrapper(this);
  }

  public EdgeIndexTableWrapper getEdgeIndexWrapper() {
    return new EdgeIndexTableWrapper(this);
  }

  public IndexedKeysTableWrapper getKeyMetadataWrapper() {
    return new IndexedKeysTableWrapper(this);
  }

  public IndexNameTableWrapper getIndexMetadataWrapper() {
    return new IndexNameTableWrapper(this);
  }

  public <T extends Element> ElementTableWrapper getElementWrapper(Class<T> clazz) {
    if (Vertex.class.equals(clazz)) {
      return getVertexWrapper();
    } else if (Edge.class.equals(clazz)) {
      return getEdgeWrapper();
    } else {
      throw new AccumuloGraphException("Unrecognized class: "+clazz);
    }
  }

  public <T extends Element> IndexTableWrapper getKeyIndexWrapper(Class<T> clazz) {
    if (Vertex.class.equals(clazz)) {
      return getVertexIndexWrapper();
    } else if (Edge.class.equals(clazz)) {
      return getEdgeIndexWrapper();
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
}
