/******************************************************************************
 *                              COPYRIGHT NOTICE                              *
 * Copyright (c) 2014 The Johns Hopkins University/Applied Physics Laboratory *
 *                            All rights reserved.                            *
 *                                                                            *
 * This material may only be used, modified, or reproduced by or for the      *
 * U.S. Government pursuant to the license rights granted under FAR clause    *
 * 52.227-14 or DFARS clauses 252.227-7013/7014.                              *
 *                                                                            *
 * For any other permissions, please contact the Legal Office at JHU/APL.     *
 ******************************************************************************/
package edu.jhuapl.tinkerpop;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * Class responsible for Element-based table operations.
 */
abstract class ElementTableOperations {

  private AccumuloGraphConfiguration config;
  private Class<? extends Element> type;
  private MultiTableBatchWriter mtbw;

  public ElementTableOperations(AccumuloGraphConfiguration config,
      MultiTableBatchWriter writer, Class<? extends Element> elementType) {
    this.config = config;
    this.type = elementType;
    this.mtbw = writer;
  }

  /**
   * Read the given property from the backing table
   * for the given element id.
   * @param id
   * @param key
   * @return
   */
  protected <V> V readProperty(String id, String key) {
    Scanner s = getElementScanner();

    s.setRange(new Range(id));

    Text colf = null;
    if (StringFactory.LABEL.equals(key)) {
      colf = AccumuloGraph.TLABEL;
    } else {
      colf = new Text(key);
    }
    s.fetchColumnFamily(colf);

    V value = null;

    Iterator<Entry<Key, Value>> iter = s.iterator();
    if (iter.hasNext()) {
      value = AccumuloByteSerializer.deserialize(iter.next().getValue().get());
    }
    s.close();

    return value;
  }

  /**
   * Get all property keys for the given element id.
   * @param id
   * @return
   */
  protected Set<String> readPropertyKeys(String id) {
    Scanner s = getElementScanner();

    s.setRange(new Range(id));

    Set<String> keys = new HashSet<String>();

    Iterator<Entry<Key,Value>> iter = s.iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> e = iter.next();
      Key k = e.getKey();
      String cf = k.getColumnFamily().toString();
      keys.add(cf);
    }

    s.close();

    // Remove some special keys.
    keys.remove(AccumuloGraph.TINEDGE.toString());
    keys.remove(AccumuloGraph.TLABEL.toString());
    keys.remove(AccumuloGraph.TOUTEDGE.toString());

    return keys;
  }

  protected void clearProperty(String id, String key) {
    try {
      Mutation m = new Mutation(id);
      m.putDelete(key.getBytes(), AccumuloGraph.EMPTY);
      BatchWriter bw = getBatchWriter();
      bw.addMutation(m);

    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }

  private Scanner getElementScanner() {
    try {
      return config.getConnector().createScanner(type.equals(Vertex.class) ?
          config.getVertexTable() : config.getEdgeTable(), config.getAuthorizations());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private BatchWriter getBatchWriter() {
    try {
      return mtbw.getBatchWriter(type.equals(Vertex.class) ?
          config.getVertexTable() : config.getEdgeTable());
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  public void close() {
    // TODO?
  }
}
