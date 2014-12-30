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

import java.util.HashMap;
import java.util.Map;

/**
 * Cache for storing element properties.
 * This supports a global timeout for key evictions,
 * as well as per-property eviction.
 * If caching is disabled for the given property,
 * this does nothing (stores no values).
 */
public class PropertyCache {

  private final AccumuloGraphConfiguration cfg;
  private final Map<String, TimedValue> values;

  public PropertyCache(AccumuloGraphConfiguration cfg) {
    this.cfg = cfg;
    this.values = new HashMap<String, TimedValue>();
  }

  public void put(String key, Object value) {
    Integer timeout = getTimeout(key);

    // Don't cache anything without a specified timeout.
    if (timeout == null) {
      return;
    }
    values.put(key, new TimedValue(value,
        timeout != null ? System.currentTimeMillis() + timeout : null));
  }

  public void putAll(Map<String, Object> entries) {
    for (String key : entries.keySet()) {
      put(key, entries.get(key));
    }
  }

  public <T> T get(String key) {
    long now = System.currentTimeMillis();

    TimedValue val = values.get(key);
    if (val != null) {
      if (val.getExpiry() != null &&
          val.getExpiry() <= now) {
        remove(key);
        return null;
      }
      else {
        return (T) val.getValue();
      }
    }

    return null;
  }

  public void remove(String key) {
    values.remove(key);
  }

  public void clear() {
    values.clear();
  }

  /**
   * Return the timeout for the given key.
   * Checks for a key-specific timeout
   * first, then the default, if any.
   * Return null if no timeout.
   * @param key
   * @return
   */
  private Integer getTimeout(String key) {
    int timeout = cfg.getPropertyCacheTimeout(key);
    if (timeout <= 0) {
      timeout = cfg.getPropertyCacheTimeout(null);
    }
    return timeout > 0 ? timeout : null;
  }

  /**
   * Value with associated expiry time.
   */
  private static class TimedValue {
    private final Object value;
    private final Long expiry;

    public TimedValue(Object value, Long expiry) {
      this.value = value;
      this.expiry = expiry;
    }

    public Object getValue() {
      return value;
    }

    public Long getExpiry() {
      return expiry;
    }
  }
}
