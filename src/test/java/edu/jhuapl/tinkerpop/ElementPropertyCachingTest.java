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

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;

/**
 * Tests related to {@link Element}-based property
 * loading and caching.
 */
public class ElementPropertyCachingTest {

  private static final int TIMEOUT = 300000;
  private static final String NON_CACHED = "noncached";
  private static final String CACHED = "cached";

  @Test
  public void testCachingDisabled() {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("cachingDisabled");
    assertTrue(cfg.getPropertyCacheTimeout(null) <= 0);
    assertTrue(cfg.getPropertyCacheTimeout(NON_CACHED) <= 0);
    assertTrue(cfg.getPropertyCacheTimeout(CACHED) <= 0);

    Graph graph = open(cfg);
    load(graph);

    AccumuloVertex a = (AccumuloVertex) graph.getVertex("A");
    AccumuloVertex b = (AccumuloVertex) graph.getVertex("B");
    AccumuloVertex c = (AccumuloVertex) graph.getVertex("C");

    assertEquals(null, a.getProperty(NON_CACHED));
    assertEquals(true, b.getProperty(NON_CACHED));
    assertEquals(null, c.getProperty(NON_CACHED));
    assertEquals(null, a.getProperty(CACHED));
    assertEquals(null, b.getProperty(CACHED));
    assertEquals(true, c.getProperty(CACHED));

    assertEquals(null, a.getPropertyCache().get(NON_CACHED));
    assertEquals(null, b.getPropertyCache().get(NON_CACHED));
    assertEquals(null, c.getPropertyCache().get(NON_CACHED));
    assertEquals(null, a.getPropertyCache().get(CACHED));
    assertEquals(null, b.getPropertyCache().get(CACHED));
    assertEquals(null, c.getPropertyCache().get(CACHED));

    assertEquals(Sets.newHashSet(), a.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), b.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), c.getPropertyCache().keySet());

    a.removeProperty(NON_CACHED);
    b.removeProperty(NON_CACHED);
    c.removeProperty(NON_CACHED);
    a.removeProperty(CACHED);
    b.removeProperty(CACHED);
    c.removeProperty(CACHED);

    assertEquals(null, a.getProperty(NON_CACHED));
    assertEquals(null, b.getProperty(NON_CACHED));
    assertEquals(null, c.getProperty(NON_CACHED));
    assertEquals(null, a.getProperty(CACHED));
    assertEquals(null, b.getProperty(CACHED));
    assertEquals(null, c.getProperty(CACHED));

    assertEquals(null, a.getPropertyCache().get(NON_CACHED));
    assertEquals(null, b.getPropertyCache().get(NON_CACHED));
    assertEquals(null, c.getPropertyCache().get(NON_CACHED));
    assertEquals(null, a.getPropertyCache().get(CACHED));
    assertEquals(null, b.getPropertyCache().get(CACHED));
    assertEquals(null, c.getPropertyCache().get(CACHED));

    assertEquals(Sets.newHashSet(), a.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), b.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), c.getPropertyCache().keySet());

    graph.shutdown();
  }

  @Test
  public void testSpecificCaching() {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("getProperty");
    cfg.setPropertyCacheTimeout(CACHED, TIMEOUT);

    assertTrue(cfg.getPropertyCacheTimeout(null) <= 0);
    assertTrue(cfg.getPropertyCacheTimeout(NON_CACHED) <= 0);
    assertEquals(TIMEOUT, cfg.getPropertyCacheTimeout(CACHED));

    Graph graph = open(cfg);
    load(graph);

    AccumuloVertex a = (AccumuloVertex) graph.getVertex("A");
    AccumuloVertex b = (AccumuloVertex) graph.getVertex("B");
    AccumuloVertex c = (AccumuloVertex) graph.getVertex("C");

    assertEquals(null, a.getProperty(NON_CACHED));
    assertEquals(true, b.getProperty(NON_CACHED));
    assertEquals(null, c.getProperty(NON_CACHED));
    assertEquals(null, a.getProperty(CACHED));
    assertEquals(null, b.getProperty(CACHED));
    assertEquals(true, c.getProperty(CACHED));

    assertEquals(null, a.getPropertyCache().get(NON_CACHED));
    assertEquals(null, b.getPropertyCache().get(NON_CACHED));
    assertEquals(null, c.getPropertyCache().get(NON_CACHED));
    assertEquals(null, a.getPropertyCache().get(CACHED));
    assertEquals(null, b.getPropertyCache().get(CACHED));
    assertEquals(true, c.getPropertyCache().get(CACHED));

    assertEquals(Sets.newHashSet(), a.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), b.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(CACHED), c.getPropertyCache().keySet());

    a.removeProperty(NON_CACHED);
    b.removeProperty(NON_CACHED);
    c.removeProperty(NON_CACHED);
    a.removeProperty(CACHED);
    b.removeProperty(CACHED);
    c.removeProperty(CACHED);

    assertEquals(null, a.getProperty(NON_CACHED));
    assertEquals(null, b.getProperty(NON_CACHED));
    assertEquals(null, c.getProperty(NON_CACHED));
    assertEquals(null, a.getProperty(CACHED));
    assertEquals(null, b.getProperty(CACHED));
    assertEquals(null, c.getProperty(CACHED));

    assertEquals(null, a.getPropertyCache().get(NON_CACHED));
    assertEquals(null, b.getPropertyCache().get(NON_CACHED));
    assertEquals(null, c.getPropertyCache().get(NON_CACHED));
    assertEquals(null, a.getPropertyCache().get(CACHED));
    assertEquals(null, b.getPropertyCache().get(CACHED));
    assertEquals(null, c.getPropertyCache().get(CACHED));

    assertEquals(Sets.newHashSet(), a.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), b.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), c.getPropertyCache().keySet());

    graph.shutdown();
  }

  @Test
  public void testAllCaching() {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("setProperty");
    cfg.setPropertyCacheTimeout(null, TIMEOUT);
    cfg.setPropertyCacheTimeout(CACHED, TIMEOUT);

    assertEquals(TIMEOUT, cfg.getPropertyCacheTimeout(null));
    assertEquals(TIMEOUT, cfg.getPropertyCacheTimeout(NON_CACHED));
    assertEquals(TIMEOUT, cfg.getPropertyCacheTimeout(CACHED));

    Graph graph = open(cfg);
    load(graph);

    AccumuloVertex a = (AccumuloVertex) graph.getVertex("A");
    AccumuloVertex b = (AccumuloVertex) graph.getVertex("B");
    AccumuloVertex c = (AccumuloVertex) graph.getVertex("C");

    assertEquals(null, a.getProperty(NON_CACHED));
    assertEquals(true, b.getProperty(NON_CACHED));
    assertEquals(null, c.getProperty(NON_CACHED));
    assertEquals(null, a.getProperty(CACHED));
    assertEquals(null, b.getProperty(CACHED));
    assertEquals(true, c.getProperty(CACHED));

    assertEquals(null, a.getPropertyCache().get(NON_CACHED));
    assertEquals(true, b.getPropertyCache().get(NON_CACHED));
    assertEquals(null, c.getPropertyCache().get(NON_CACHED));
    assertEquals(null, a.getPropertyCache().get(CACHED));
    assertEquals(null, b.getPropertyCache().get(CACHED));
    assertEquals(true, c.getPropertyCache().get(CACHED));

    assertEquals(Sets.newHashSet(), a.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(NON_CACHED), b.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(CACHED), c.getPropertyCache().keySet());

    a.removeProperty(NON_CACHED);
    b.removeProperty(NON_CACHED);
    c.removeProperty(NON_CACHED);
    a.removeProperty(CACHED);
    b.removeProperty(CACHED);
    c.removeProperty(CACHED);

    assertEquals(null, a.getProperty(NON_CACHED));
    assertEquals(null, b.getProperty(NON_CACHED));
    assertEquals(null, c.getProperty(NON_CACHED));
    assertEquals(null, a.getProperty(CACHED));
    assertEquals(null, b.getProperty(CACHED));
    assertEquals(null, c.getProperty(CACHED));

    assertEquals(null, a.getPropertyCache().get(NON_CACHED));
    assertEquals(null, b.getPropertyCache().get(NON_CACHED));
    assertEquals(null, c.getPropertyCache().get(NON_CACHED));
    assertEquals(null, a.getPropertyCache().get(CACHED));
    assertEquals(null, b.getPropertyCache().get(CACHED));
    assertEquals(null, c.getPropertyCache().get(CACHED));

    assertEquals(Sets.newHashSet(), a.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), b.getPropertyCache().keySet());
    assertEquals(Sets.newHashSet(), c.getPropertyCache().keySet());

    graph.shutdown();
  }

  @Test
  public void testPreloadSomeProperties() {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("preloadSomeProperties");
    cfg.setPropertyCacheTimeout(null, TIMEOUT);
    cfg.setPreloadedProperties(new String[]{CACHED});

    Graph graph = open(cfg);

    AccumuloVertex v = (AccumuloVertex) graph.addVertex("V");
    v.setProperty(NON_CACHED, true);
    v.setProperty(CACHED, true);

    v = (AccumuloVertex) graph.getVertex("V");
    assertEquals(null, v.getPropertyInMemory(NON_CACHED));
    assertEquals(true, v.getPropertyInMemory(CACHED));

    graph.shutdown();
  }

  private static Graph open(AccumuloGraphConfiguration cfg) {
    return GraphFactory.open(cfg);
  }

  private static void load(Graph graph) {
    graph.addVertex("A");
    graph.addVertex("B").setProperty(NON_CACHED, true);
    graph.addVertex("C").setProperty(CACHED, true);
  }
}
