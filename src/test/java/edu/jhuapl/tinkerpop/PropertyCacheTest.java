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

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test the {@link PropertyCache} object.
 */
public class PropertyCacheTest {

  @Test
  public void testUncachedProperty() throws Exception {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("uncached");
    PropertyCache cache = new PropertyCache(cfg);
    cache.put("K1", "V1");
    cache.put("K2", "V2");
    cache.put("K3", "V3");
    assertNull(cache.get("K1"));
    assertNull(cache.get("K2"));
    assertNull(cache.get("K3"));
  }

  @Test
  public void testDefaultCachedProperty() throws Exception {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("defaultCached");
    cfg.setPropertyCacheTimeout(null, 1000);

    PropertyCache cache = new PropertyCache(cfg);
    cache.put("K1", "V1");
    assertEquals("V1", cache.get("K1"));
    Thread.sleep(1500);
    assertNull(cache.get("K1"));

    cache.put("K2", "V2");
    assertEquals("V2", cache.get("K2"));
    cache.remove("K2");
    assertNull(cache.get("K2"));

    cache.put("K3", "V3");
    cache.clear();
    assertNull(cache.get("K3"));
  }

  @Test
  public void testSpecificCachedProperty() throws Exception {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("specificCached");
    cfg.setPropertyCacheTimeout(null, 1000);
    cfg.setPropertyCacheTimeout("long", 2000);
    cfg.setPropertyCacheTimeout("longer", 3000);

    PropertyCache cache = new PropertyCache(cfg);
    cache.put("default", "V1");
    cache.put("long", "V2");
    cache.put("longer", "V3");

    assertEquals("V1", cache.get("default"));
    assertEquals("V2", cache.get("long"));
    assertEquals("V3", cache.get("longer"));

    Thread.sleep(1500);
    assertNull(cache.get("default"));
    Thread.sleep(1000);
    assertNull(cache.get("long"));
    Thread.sleep(1000);
    assertNull(cache.get("longer"));
  }
}
