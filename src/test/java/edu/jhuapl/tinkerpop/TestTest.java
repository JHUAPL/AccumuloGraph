package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

public class TestTest {
  @org.junit.Test
  public void Test(){
    AccumuloGraph ag = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils.generateGraphConfig("testetst").getConfiguration());
    
    ag.addVertex("1");
    ag.addVertex("2");
    ag.addVertex("3");
    ag.addVertex("4");
    ag.addVertex("5");
    int c = 0;
    for (Vertex v : ag.getVertices()){
      c++;
    }
    System.out.println(c);  
  }
  
}
