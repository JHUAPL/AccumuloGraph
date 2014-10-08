package edu.jhuapl.tinkerpop;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

public class AutoIndexTest {

  @Test
  public void testIndexCreation() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
    AccumuloGraph ag = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils.generateGraphConfig("AutoIndexTest").setAutoIndex(true).getConfiguration());
    String VERT = "1234";
    String KEY = "name";
    String VALUE = "bananaman";

    Vertex v1 = ag.addVertex(VERT);
    v1.setProperty(KEY, VALUE);

    Scanner scan = ag.getVertexIndexScanner();
    for (Entry<Key,Value> kv : scan) {
      assertEquals(new String(AccumuloByteSerializer.serialize(VALUE)), kv.getKey().getRow().toString());
      assertEquals(KEY, kv.getKey().getColumnFamily().toString());
      assertEquals(VERT, kv.getKey().getColumnQualifier().toString());
    }

  }

  @Test
  public void testRegularCreation() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
    AccumuloGraph ag = (AccumuloGraph) GraphFactory.open(AccumuloGraphTestUtils.generateGraphConfig("NoAutoIndexTest").getConfiguration());
    String VERT = "1234";
    String KEY = "name";
    String VALUE = "bananaman";

    Vertex v1 = ag.addVertex(VERT);
    v1.setProperty(KEY, VALUE);

    Scanner scan = ag.getVertexIndexScanner();
    for (Entry<Key,Value> kv : scan) {
      assertTrue(false);
    }

  }

}
