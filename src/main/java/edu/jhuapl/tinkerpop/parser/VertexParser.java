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
package edu.jhuapl.tinkerpop.parser;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * TODO
 */
public class VertexParser extends ElementParser<AccumuloVertex> {

  public VertexParser(GlobalInstances globals) {
    super(globals);
  }

  @Override
  public AccumuloVertex parse(String id, Iterable<Entry<Key,Value>> entries) {
    AccumuloVertex vertex = new AccumuloVertex(globals, id);
    setInMemoryProperties(vertex, entries);
    return vertex;
  }
}
