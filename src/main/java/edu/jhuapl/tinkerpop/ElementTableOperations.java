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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
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

  public ElementTableOperations(AccumuloGraphConfiguration config,
      Class<? extends Element> elementType) {
    this.config = config;
    this.type = elementType;
  }

  protected <V> V getProperty(String id, String key) {
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

  private Scanner getElementScanner() {
    try {
      return config.getConnector().createScanner(type.equals(Vertex.class) ?
          config.getVertexTable() : config.getEdgeTable(), config.getAuthorizations());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    // TODO
  }
}
