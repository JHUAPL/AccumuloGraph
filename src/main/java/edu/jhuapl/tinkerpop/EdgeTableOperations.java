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

import org.apache.accumulo.core.client.MultiTableBatchWriter;

import com.tinkerpop.blueprints.Edge;


/**
 * TODO
 */
public class EdgeTableOperations extends ElementTableOperations {

  public EdgeTableOperations(AccumuloGraphConfiguration config,
      MultiTableBatchWriter writer) {
    super(config, writer, Edge.class);
  }
}
