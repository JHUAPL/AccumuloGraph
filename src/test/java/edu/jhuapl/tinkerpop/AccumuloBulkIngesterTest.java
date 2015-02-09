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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class AccumuloBulkIngesterTest {

  @Test
  public void testBulkIngester() throws Exception {
    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils.generateGraphConfig("propertyBuilder").setClear(true);

    AccumuloBulkIngester ingester = new AccumuloBulkIngester(cfg);

    for (String t : cfg.getTableNames()) {
      assertTrue(cfg.getConnector().tableOperations().exists(t));
    }

    ingester.addVertex("A").finish();
    ingester.addVertex("B").add("P1", "V1").add("P2", "2").finish();
    ingester.addEdge("A", "B", "edge").add("P3", "V3").finish();
    ingester.shutdown(true);

    AccumuloGraph graph = new AccumuloGraph(cfg.clone().setClear(false));
    Vertex v1 = graph.getVertex("A");
    assertNotNull(v1);

    Iterator<Edge> it = v1.getEdges(Direction.OUT).iterator();
    assertTrue(it.hasNext());

    Edge e = it.next();
    assertEquals("edge", e.getLabel());

    Vertex v2 = e.getVertex(Direction.IN);
    assertEquals("B", v2.getId());
    assertEquals("V1", v2.getProperty("P1"));
    assertEquals("2", v2.getProperty("P2"));

    graph.shutdown();
  }

}
