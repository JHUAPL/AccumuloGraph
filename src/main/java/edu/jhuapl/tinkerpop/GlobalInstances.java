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
import edu.jhuapl.tinkerpop.tables.BaseIndexValuesTableWrapper;
import edu.jhuapl.tinkerpop.tables.core.EdgeTableWrapper;
import edu.jhuapl.tinkerpop.tables.core.ElementTableWrapper;
import edu.jhuapl.tinkerpop.tables.core.VertexTableWrapper;
import edu.jhuapl.tinkerpop.tables.keyindex.EdgeKeyIndexTableWrapper;
import edu.jhuapl.tinkerpop.tables.keyindex.IndexedKeysListTableWrapper;
import edu.jhuapl.tinkerpop.tables.keyindex.VertexKeyIndexTableWrapper;
import edu.jhuapl.tinkerpop.tables.namedindex.NamedIndexListTableWrapper;

/**
 * Internal class gathering together instances of
 * objects needed for various AccumuloGraph components.
 */
public class GlobalInstances {

  private final AccumuloGraphConfiguration config;
  private final MultiTableBatchWriter mtbw;
  private final ElementCaches caches;

  public GlobalInstances(AccumuloGraphConfiguration config,
      MultiTableBatchWriter mtbw, ElementCaches caches) {
    this.config = config;
    this.mtbw = mtbw;
    this.caches = caches;
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

  public VertexKeyIndexTableWrapper getVertexKeyIndexWrapper() {
    return new VertexKeyIndexTableWrapper(this);
  }

  public EdgeKeyIndexTableWrapper getEdgeKeyIndexWrapper() {
    return new EdgeKeyIndexTableWrapper(this);
  }

  public IndexedKeysListTableWrapper getIndexedKeysListWrapper() {
    return new IndexedKeysListTableWrapper(this);
  }

  public NamedIndexListTableWrapper getNamedIndexListWrapper() {
    return new NamedIndexListTableWrapper(this);
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

  public <T extends Element> BaseIndexValuesTableWrapper getIndexValuesWrapper(Class<T> clazz) {
    if (Vertex.class.equals(clazz)) {
      return getVertexKeyIndexWrapper();
    } else if (Edge.class.equals(clazz)) {
      return getEdgeKeyIndexWrapper();
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
