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

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.StringFactory;

public abstract class AccumuloElement implements Element {

  protected GlobalInstances globals;
  protected String id;

  private Class<? extends Element> type;

  private PropertyCache propertyCache;

  protected AccumuloElement(GlobalInstances globals,
      String id, Class<? extends Element> type) {
    this.globals = globals;
    this.id = id;
    this.type = type;
  }

  /**
   * Create properties cache if it doesn't exist,
   * and preload any properties.
   */
  private void makeCache() {
    if (propertyCache == null) {
      propertyCache = new PropertyCache(globals.getConfig());

      // Preload any keys, if needed.
      String[] preloadKeys = globals.getConfig().getPreloadedProperties();
      if (preloadKeys != null) {
        propertyCache.putAll(globals.getElementWrapper(type)
            .readProperties(this, preloadKeys));
      }
    }
  }

  @Override
  public <T> T getProperty(String key) {
    makeCache();

    // Get from property cache.
    T value = propertyCache.get(key);

    // If not cached, get it from the backing table.
    if (value == null) {
      value = globals.getElementWrapper(type).readProperty(this, key);
    }

    // Cache the new value.
    if (value != null) {
      propertyCache.put(key, value);
    }

    return value;
  }

  @Override
  public Set<String> getPropertyKeys() {
    return globals.getElementWrapper(type).readPropertyKeys(this);
  }

  @Override
  public void setProperty(String key, Object value) {
    makeCache();
    globals.getGraph().setPropertyForIndexes(type, this, key, value);
    // MDL 31 Dec 2014:  The above calls getProperty, so this
    //   order is important (for now).
    globals.getElementWrapper(type).writeProperty(this, key, value);
    globals.getGraph().checkedFlush();
    propertyCache.put(key, value);
  }

  @Override
  public <T> T removeProperty(String key) {
    if (StringFactory.LABEL.equals(key) ||
        AccumuloGraph.SLABEL.equals(key)) {
      throw new AccumuloGraphException("Cannot remove the " + StringFactory.LABEL + " property.");
    }

    makeCache();
    T value = getProperty(key);
    if (value != null) {
      globals.getElementWrapper(type).clearProperty(this, key);
      globals.getGraph().checkedFlush();
    }
    globals.getGraph().removePropertyFromIndex(type, this, key, value);
    // MDL 31 Dec 2014:  AccumuloGraph.removeProperty
    //   calls getProperty which populates the cache.
    //   So the order here is important (for now).
    propertyCache.remove(key);
    return value;
  }

  @Override
  public Object getId() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    } else if (!obj.getClass().equals(getClass())) {
      return false;
    } else {
      return id.equals(((AccumuloElement) obj).id);
    }
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() ^ id.hashCode();
  }

  /*
   * Internal method for unit tests.
   * @return
   */
  PropertyCache getPropertyCache() {
    return propertyCache;
  }

  /**
   * @deprecated This is used in {@link AccumuloGraph} but needs to go away.
   * @param key
   * @param value
   */
  void cacheProperty(String key, Object value) {
    makeCache();
    propertyCache.put(key, value);
  }

  /**
   * @deprecated This is used in {@link AccumuloGraph} but needs to go away.
   * @param props
   */
  void cacheAllProperties(Map<String, Object> props) {
    for (String key : props.keySet()) {
      cacheProperty(key, props.get(key));
    }
  }
}
