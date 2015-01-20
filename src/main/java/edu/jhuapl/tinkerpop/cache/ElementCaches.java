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
package edu.jhuapl.tinkerpop.cache;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphException;

/**
 * Utility class wrapping element caches.
 */
public class ElementCaches {
  private ElementCache<Vertex> vertexCache;
  private ElementCache<Edge> edgeCache;

  public ElementCaches(AccumuloGraphConfiguration config) {
    if (config.getVertexCacheEnabled()) {
      vertexCache = new ElementCache<Vertex>(config.getVertexCacheSize(),
          config.getVertexCacheTimeout());
    }

    if (config.getEdgeCacheEnabled()) {
      edgeCache = new ElementCache<Edge>(config.getEdgeCacheSize(),
          config.getEdgeCacheTimeout());
    }
  }

  public <T extends Element> void cache(T element, Class<T> clazz) {
    if (pick(clazz) != null) {
      pick(clazz).cache(element);
    }
  }

  public <T extends Element> T retrieve(Object id, Class<T> clazz) {
    return pick(clazz) != null ? pick(clazz).retrieve(id) : null;
  }

  public <T extends Element> void remove(Object id, Class<T> clazz) {
    if (pick(clazz) != null) {
      pick(clazz).remove(id);
    }
  }

  public <T extends Element> void clear(Class<T> clazz) {
    if (pick(clazz) != null) {
      pick(clazz).clear();
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Element> ElementCache<T> pick(Class<T> clazz) {
    if (Vertex.class.equals(clazz)) {
      return (ElementCache<T>) vertexCache;
    }
    else if (Edge.class.equals(clazz)) {
      return (ElementCache<T>) edgeCache;
    }
    else {
      throw new AccumuloGraphException("Unknown element class: "+clazz);
    }
  }
}