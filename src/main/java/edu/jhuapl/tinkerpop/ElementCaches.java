/******************************************************************************
 *                              COPYRIGHT NOTICE                              *
 * Copyright (c) 2014 The Johns Hopkins University/Applied Physics Laboratory *
 *                            All rights reserved.                            *
 *                                                                            *
 * This material may only be used, modified, or reproduced by or for the      *
 * U.S. Government pursuant to the license rights granted under FAR clause    *
 * 52.227-14 or DFARS clauses 252.227-7013/7014.                              *
 *                                                                            *
 * For any other permissions, please contact the Legal Office at JHU/APL.     *
 ******************************************************************************/
package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

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