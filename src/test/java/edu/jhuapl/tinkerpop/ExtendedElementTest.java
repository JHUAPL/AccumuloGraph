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

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

/**
 * Tests related to implementation-specific elements.
 */
public class ExtendedElementTest {

  private Graph makeGraph(AccumuloGraphConfiguration cfg) {
    return GraphFactory.open(cfg.getConfiguration());
  }

  @Test
  public void testExistenceChecks() throws Exception {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("yesExistenceChecks");
    Graph graph = makeGraph(cfg);

    String id;


    id = "doubleAdd";
    assertNotNull(graph.addVertex(id));
    try {
      graph.addVertex(id);
      fail();
    } catch (Exception e) { }

    Vertex vAdd = graph.getVertex(id);
    assertNotNull(vAdd);
    graph.removeVertex(vAdd);
    assertNull(graph.getVertex(id));


    id = "doubleRemove";
    Vertex vRemove = graph.addVertex(id);
    assertNotNull(vRemove);
    graph.removeVertex(vRemove);
    try {
      graph.removeVertex(vRemove);
      fail();
    } catch (Exception e) { }
    assertNull(graph.getVertex(id));


    id = "notExist";
    assertNull(graph.getVertex(id));


    graph.shutdown();
  }

  @Test
  public void testSkipExistenceChecks() throws Exception {
    AccumuloGraphConfiguration cfg =
        AccumuloGraphTestUtils.generateGraphConfig("skipExistenceChecks");
    cfg.setSkipExistenceChecks(true);
    Graph graph = makeGraph(cfg);

    String id;

    id = "doubleAdd";
    assertNotNull(graph.addVertex(id));
    assertNotNull(graph.addVertex(id));
    Vertex vAdd = graph.getVertex(id);
    assertNotNull(vAdd);
    graph.removeVertex(vAdd);
    assertNotNull(graph.getVertex(id));


    id = "doubleRemove";
    Vertex vRemove = graph.addVertex(id);
    assertNotNull(vRemove);
    graph.removeVertex(vRemove);
    assertNotNull(graph.getVertex(id));
    // MDL 24 Dec 2014:  removeVertex still does checks.
    //graph.removeVertex(vRemove);
    //assertNotNull(graph.getVertex(id));


    id = "notExist";
    assertNotNull(graph.getVertex(id));

    graph.shutdown();
  }
}
