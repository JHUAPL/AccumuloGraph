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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.util.Pair;

import com.tinkerpop.blueprints.Element;

public abstract class AccumuloElement implements Element {

  protected AccumuloGraph parent;
  protected String id;

  private Class type;

  private Map<String,Pair<Long,Object>> propertiesCache;

  protected AccumuloElement(AccumuloGraph parent, String id, Class<? extends Element> type) {
    this.parent = parent;
    this.id = id;
    this.type = type;
  }

  public <T> T getProperty(String key) {
    if (propertiesCache == null) {
      // lazily create the properties cache...

      // we will create it here just in case the parent does not actually
      // pre-load any data. Note it also may be created in the
      // cacheProperty method, as well, in the event a class pre-loads
      // data before a call is made to obtain it.
      propertiesCache = new HashMap<String,Pair<Long,Object>>();

      parent.preloadProperties(this, type);
    }

    Pair<Long,Object> val = propertiesCache.get(key);
    if (val != null) {
      if (val.getFirst() < System.currentTimeMillis()) {
        // this cached value has timed out..
        propertiesCache.remove(key);
      } else {
        // the cached value is still good...
        return (T) val.getSecond();
      }
    }
    Pair<Integer,T> pair = parent.getProperty(type, id, key);
    if (pair.getFirst() != null) {
      cacheProperty(key, pair.getSecond(), pair.getFirst());
    }
    return pair.getSecond();
  }

  public Set<String> getPropertyKeys() {
    return parent.getPropertyKeys(type, id);
  }

  public void setProperty(String key, Object value) {
    Integer timeout = parent.setProperty(type, id, key, value);
    cacheProperty(key, value, timeout);
  }

  public <T> T removeProperty(String key) {
    if (propertiesCache != null) {
      // we have the cached value but we still need to pass this on to the
      // parent so it can actually remove the data from the backing store.
      // Since we have to do that anyway, we will use the parent's value
      // instead of the cache value to to be as up-to-date as possible.
      // Of course we still need to clear out the cached value...
      propertiesCache.remove(key);
    }
    return parent.removeProperty(type, id, key);
  }

  public Object getId() {
    return id;
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    } else if (!obj.getClass().equals(getClass())) {
      return false;
    } else {
      return this.id.equals(((AccumuloElement) obj).id);
    }
  }

  public int hashCode() {
    return getClass().hashCode() ^ id.hashCode();
  }

  void cacheProperty(String key, Object value, Integer timeoutMillis) {
    if (timeoutMillis == null) {
      // user does not want to cache data...
      return;
    }

    if (propertiesCache == null) {
      propertiesCache = new HashMap<String,Pair<Long,Object>>();
    }
    Pair<Long,Object> tsVal = new Pair<Long,Object>(System.currentTimeMillis() + timeoutMillis, value);
    propertiesCache.put(key, tsVal);
  }

}
