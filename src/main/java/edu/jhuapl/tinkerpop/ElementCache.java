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
