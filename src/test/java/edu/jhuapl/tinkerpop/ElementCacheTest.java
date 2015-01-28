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

import org.junit.Test;

import static org.junit.Assert.*;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.cache.ElementCache;

public class ElementCacheTest {

  @Test
  public void testElementCacheSize() throws Exception {
    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils
        .generateGraphConfig("elementCacheSize");
    Graph graph = GraphFactory.open(cfg.getConfiguration());

    Vertex[] verts = new Vertex[10];
    for (int i = 0; i < verts.length; i++) {
      verts[i] = graph.addVertex(i);
    }

    Edge[] edges = new Edge[9];
    for (int i = 0; i < edges.length; i++) {
      edges[i] = graph.addEdge(null,
          verts[0], verts[i+1], "edge");
    }

    sizeTests(verts);
    sizeTests(edges);

    graph.shutdown();
  }

  private void sizeTests(Element[] elts) {
    ElementCache<Element> cache =
        new ElementCache<Element>(3, 120000);
    for (Element e : elts) {
      cache.cache(e);
    }

    for (Element e : elts) {
      cache.cache(e);
    }

    int total = 0;
    for (Element e : elts) {
      if (cache.retrieve(e.getId()) != null) {
        total++;
      }
    }
    assertTrue(total < elts.length);

    cache.clear();
    for (Element e : elts) {
      assertNull(cache.retrieve(e.getId()));
    }
  }

  @Test
  public void testElementCacheTimeout() throws Exception {
    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils
        .generateGraphConfig("elementCacheTimeout");
    Graph graph = GraphFactory.open(cfg.getConfiguration());

    ElementCache<Element> cache =
        new ElementCache<Element>(10, 1000);

    Vertex v1 = graph.addVertex(1);
    Vertex v2 = graph.addVertex(2);
    assertNull(cache.retrieve(1));
    assertNull(cache.retrieve(2));

    cache.cache(v1);
    assertNotNull(cache.retrieve(v1.getId()));
    Thread.sleep(1500);
    assertNull(cache.retrieve(v1.getId()));

    Edge e = graph.addEdge(null, v1, v2, "label");
    assertNull(cache.retrieve(e.getId()));

    cache.cache(e);
    assertNotNull(cache.retrieve(e.getId()));
    Thread.sleep(1500);
    assertNull(cache.retrieve(e.getId()));

    graph.shutdown();
  }
}
