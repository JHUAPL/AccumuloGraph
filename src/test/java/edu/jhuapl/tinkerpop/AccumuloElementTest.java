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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

/**
 * Tests related to Accumulo elements.
 */
public class AccumuloElementTest {

  @Test
  public void testNonStringIds() throws Exception {
    Graph graph = AccumuloGraphTestUtils.makeGraph("nonStringIds");

    Object[] ids = new Object[] {
        10, 20, 30L, 40L,
        50.0f, 60.0f, 70.0d, 80.0d,
        (byte) 'a', (byte) 'b', 'c', 'd',
        "str1", "str2",
        new GenericObject("str3"), new GenericObject("str4"),
    };

    Object[] edgeIds = new Object[] {
        100, 200, 300L, 400L,
        500.0f, 600.0f, 700.0d, 800.0d,
        (byte) 'e', (byte) 'f', 'g', 'h',
        "str5", "str6",
        new GenericObject("str7"), new GenericObject("str8"),
    };

    for (int i = 0; i < ids.length; i++) {
      assertNull(graph.getVertex(ids[i]));
      Vertex v = graph.addVertex(ids[i]);
      assertNotNull(v);
      assertNotNull(graph.getVertex(ids[i]));
    }
    assertEquals(ids.length, count(graph.getVertices()));

    for (int i = 1; i < edgeIds.length; i++) {
      assertNull(graph.getEdge(edgeIds[i-1]));
      Edge e = graph.addEdge(edgeIds[i-1],
          graph.getVertex(ids[i-1]),
          graph.getVertex(ids[i]), "label");
      assertNotNull(e);
      assertNotNull(graph.getEdge(edgeIds[i-1]));
    }
    assertEquals(edgeIds.length-1, count(graph.getEdges()));

    graph.shutdown();
  }

  private static int count(Iterable<?> iter) {
    int count = 0;
    for (@SuppressWarnings("unused") Object obj : iter) {
      count++;
    }
    return count;
  }

  private static class GenericObject {
    private final Object id;

    public GenericObject(Object id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return "GenericObject [id=" + id + "]";
    }
  }
}
