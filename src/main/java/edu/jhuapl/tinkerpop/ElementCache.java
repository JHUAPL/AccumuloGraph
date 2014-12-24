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

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tinkerpop.blueprints.Element;

/**
 * Simple cache for retrieved graph elements,
 * backed by Guava's cache implementation.
 */
public class ElementCache<T extends Element> {

  private Cache<Object, T> cache;

  public ElementCache(int size, int timeout) {
    cache = CacheBuilder.newBuilder()
        .maximumSize(size)
        .expireAfterAccess(timeout, TimeUnit.MILLISECONDS)
        .build();
  }

  public void cache(T element) {
    cache.put(element.getId(), element);
  }

  public T retrieve(Object id) {
    return cache.getIfPresent(id);
  }

  public void remove(Object id) {
    cache.invalidate(id);
  }

  public void clear() {
    cache.invalidateAll();
  }
}
