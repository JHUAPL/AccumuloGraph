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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class AccumuloGraphConfigurationTest {

  @Test
  public void testConfigurationInterface() throws Exception {
    Configuration conf = AccumuloGraphTestUtils.generateGraphConfig("setPropsValid");
    for (String key : AccumuloGraphConfiguration.getValidInternalKeys()) {
      // This is bad... but we should allow them if they are valid keys.
      conf.setProperty(key, "value");
    }

    conf = AccumuloGraphTestUtils.generateGraphConfig("setPropsInvalid");
    try {
      conf.setProperty("invalidKey", "value");
      fail();
    } catch (Exception e) { }
  }

  @Test
  public void testSplits() throws Exception {
    AccumuloGraphConfiguration cfg;

    // Tests for splits string.
    cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplits").setSplits((String) null);
    AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
    for (String table : cfg.getTableNames()) {
      assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
    }
    graph.shutdown();

    cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplits").setSplits("");
    graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
    for (String table : cfg.getTableNames()) {
      assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
    }
    graph.shutdown();

    cfg = AccumuloGraphTestUtils.generateGraphConfig("threeSplits").setSplits(" a b c ");
    graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
    for (String table : cfg.getTableNames()) {
      Collection<Text> splits = cfg.getConnector().tableOperations().listSplits(table);
      assertEquals(3, splits.size());
      List<Text> arr = new ArrayList<Text>(splits);
      assertEquals("a", arr.get(0).toString());
      assertEquals("b", arr.get(1).toString());
      assertEquals("c", arr.get(2).toString());
    }
    graph.shutdown();

    // Tests for splits array.
    cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplitsArray").setSplits((String[]) null);
    graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
    for (String table : cfg.getTableNames()) {
      assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
    }
    graph.shutdown();

    cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplitsArray").setSplits(new String[] {});
    graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
    for (String table : cfg.getTableNames()) {
      assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
    }
    graph.shutdown();

    cfg = AccumuloGraphTestUtils.generateGraphConfig("threeSplitsArray").setSplits(new String[] {"d", "e", "f"});
    graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
    for (String table : cfg.getTableNames()) {
      Collection<Text> splits = cfg.getConnector().tableOperations().listSplits(table);
      assertEquals(3, splits.size());
      List<Text> arr = new ArrayList<Text>(splits);
      assertEquals("d", arr.get(0).toString());
      assertEquals("e", arr.get(1).toString());
      assertEquals("f", arr.get(2).toString());
    }
    graph.shutdown();
  }

  @Test
  public void testPropertyValues() throws Exception {
    AccumuloGraph graph = new AccumuloGraph(AccumuloGraphTestUtils.generateGraphConfig("propertyValues"));
    // Tests for serialization/deserialization of properties.
    QName qname = new QName("ns", "prop");
    Vertex v = graph.addVertex(null);
    v.setProperty("qname", qname);
    assertTrue(v.getProperty("qname") instanceof QName);
    assertTrue(qname.equals(v.getProperty("qname")));
  }

  @Test
  public void testIsEmpty() throws Exception {
    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils.generateGraphConfig("isEmpty");
    AccumuloGraph graph = new AccumuloGraph(cfg);
    assertTrue(graph.isEmpty());

    graph.addVertex("A");
    assertFalse(graph.isEmpty());

    graph.clear();
    assertTrue(graph.isEmpty());
  }

  @Test
  public void testCreateAndClear() throws Exception {
    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils.generateGraphConfig("noCreate").setCreate(false);
    try {
      new AccumuloGraph(cfg);
      fail("Create is disabled and graph does not exist");
    } catch (Exception e) {
      assertTrue(true);
    }

    cfg = AccumuloGraphTestUtils.generateGraphConfig("yesCreate").setCreate(true);
    for (String t : cfg.getTableNames()) {
      assertFalse(cfg.getConnector().tableOperations().exists(t));
    }
    AccumuloGraph graph = new AccumuloGraph(cfg);
    for (String t : cfg.getTableNames()) {
      assertTrue(cfg.getConnector().tableOperations().exists(t));
    }
    graph.shutdown();

    graph = new AccumuloGraph(cfg.setCreate(false));
    assertTrue(graph.isEmpty());
    graph.addVertex("A");
    graph.addVertex("B");
    assertFalse(graph.isEmpty());
    graph.shutdown();

    graph = new AccumuloGraph(cfg.setClear(true));
    assertTrue(graph.isEmpty());
    graph.shutdown();
  }

  @Test
  public void testPrint() throws Exception {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("printTest");
    cfg.print();
  }

  @Test
  public void testInvalidCacheParams() throws Exception {
    int size = 100;
    int timeout = 30000;

    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils
        .generateGraphConfig("cacheParams");
    cfg.validate();


    // Vertex cache.

    assertFalse(cfg.getVertexCacheEnabled());

    try {
      cfg.setVertexCacheParams(-1, timeout);
      fail();
    } catch (Exception e) { }

    try {
      cfg.setVertexCacheParams(size, -1);
      fail();
    } catch (Exception e) { }

    assertFalse(cfg.getVertexCacheEnabled());

    cfg.setVertexCacheParams(size, timeout);
    cfg.validate();
    assertTrue(cfg.getVertexCacheEnabled());
    assertEquals(size, cfg.getVertexCacheSize());
    assertEquals(timeout, cfg.getVertexCacheTimeout());

    cfg.setVertexCacheParams(-1, -1);
    cfg.validate();
    assertFalse(cfg.getVertexCacheEnabled());


    // Edge cache.

    assertFalse(cfg.getEdgeCacheEnabled());

    try {
      cfg.setEdgeCacheParams(-1, timeout);
      fail();
    } catch (Exception e) { }

    try {
      cfg.setEdgeCacheParams(size, -1);
      fail();
    } catch (Exception e) { }

    assertFalse(cfg.getEdgeCacheEnabled());

    cfg.setEdgeCacheParams(size, timeout);
    cfg.validate();
    assertTrue(cfg.getEdgeCacheEnabled());
    assertEquals(size, cfg.getEdgeCacheSize());
    assertEquals(timeout, cfg.getEdgeCacheTimeout());

    cfg.setEdgeCacheParams(-1, -1);
    cfg.validate();
    assertFalse(cfg.getEdgeCacheEnabled());
  }

  @Test
  public void testBasicMockGraph() throws Exception {
    AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration()
      .setInstanceType(InstanceType.Mock)
      .setGraphName("mockGraph");

    Graph graph = GraphFactory.open(cfg.getConfiguration());

    for (int i = 0; i < 10; i++) {
      graph.addVertex(i);
    }
    assertEquals(10, count(graph.getVertices()));

    for (int i = 0; i < 9; i++) {
      System.out.println(graph.getVertex(i));
      for (int k = 1; k < 10; k++) {
        graph.addEdge(null, graph.getVertex(i),
            graph.getVertex(k), "edge");
      }
    }
    assertEquals(81, count(graph.getEdges()));

    graph.shutdown();
  }
  
  @SuppressWarnings("unused")
  private static int count(Iterable<?> iter) {
    int i = 0;
    for (Object obj : iter) {
      i++;
    }
    return i;
  }
}
