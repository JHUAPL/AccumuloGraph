package edu.jhuapl.tinkerpop;

import static org.junit.Assert.*;

import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

public class AutoIndexTest {

  @Test
  public void testVertexAutoIndex() throws Exception {
    AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils
        .generateGraphConfig("VertexAutoIndexTest").setAutoIndex(true).getConfiguration());
    String id = "1234";
    String key = "name";
    String value = "bananaman";

    Vertex v1 = graph.addVertex(id);
    v1.setProperty(key, value);

    Iterable<Element> elements = graph.getGlobals()
        .getVertexKeyIndexWrapper().readElementsFromIndex(key, value);
    int count = 0;
    for (Element element : elements) {
      assertTrue(element instanceof Vertex);
      assertEquals(id, element.getId());
      assertEquals(value, element.getProperty(key));
      count++;
    }
    assertEquals(1, count);

    graph.removeVertex(v1);
    elements = graph.getGlobals()
        .getVertexKeyIndexWrapper().readElementsFromIndex(key, value);
    assertEquals(0, count(elements));
  }

  @Test
  public void testVertexNoAutoIndex() throws Exception {
    AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils
        .generateGraphConfig("VertexNoAutoIndexTest").getConfiguration());
    String id = "1234";
    String key = "name";
    String value = "bananaman";

    Vertex v1 = graph.addVertex(id);
    v1.setProperty(key, value);

    Iterable<Element> elements = graph.getGlobals()
        .getVertexKeyIndexWrapper().readElementsFromIndex(key, value);
    assertEquals(0, count(elements));
  }

  @Test
  public void testEdgeAutoIndex() throws Exception {
    AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils
        .generateGraphConfig("EdgeAutoIndex").setAutoIndex(true).getConfiguration());
    String id1 = "A";
    String id2 = "B";
    String eid = "X";
    String key = "name";
    String value = "bananaman";

    Vertex v1 = graph.addVertex(id1);
    Vertex v2 = graph.addVertex(id2);
    Edge e = graph.addEdge(eid, v1, v2, "edge");
    e.setProperty(key, value);

    Iterable<Element> elements = graph.getGlobals()
        .getEdgeKeyIndexWrapper().readElementsFromIndex(key, value);
    int count = 0;
    for (Element element : elements) {
      assertTrue(element instanceof Edge);
      assertEquals(eid, element.getId());
      assertEquals(value, element.getProperty(key));
      count++;
    }
    assertEquals(1, count);

    graph.removeVertex(v1);
    elements = graph.getGlobals()
        .getEdgeKeyIndexWrapper().readElementsFromIndex(key, value);
    assertEquals(0, count(elements));
  }

  @Test
  public void testEdgeNoAutoIndex() throws Exception {
    AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils
        .generateGraphConfig("EdgeNoAutoIndexTest").getConfiguration());
    String id1 = "A";
    String id2 = "B";
    String eid = "X";
    String key = "name";
    String value = "bananaman";

    Vertex v1 = graph.addVertex(id1);
    Vertex v2 = graph.addVertex(id2);
    Edge e = graph.addEdge(eid, v1, v2, "edge");
    e.setProperty(key, value);

    Iterable<Element> elements = graph.getGlobals()
        .getEdgeKeyIndexWrapper().readElementsFromIndex(key, value);
    assertEquals(0, count(elements));
  }

  @SuppressWarnings("unused")
  private static int count(Iterable<?> it) {
    int count = 0;
    for (Object obj : it) {
      count++;
    }
    return count;
  }
}
