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
package edu.jhuapl.tinkerpop.tables;

import org.apache.accumulo.core.client.MultiTableBatchWriter;

import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;

/**
 * Wrapper around {@link Vertex} index table.
 */
public class VertexIndexTableWrapper extends IndexTableWrapper {

  /**
   * @param config
   * @param mtbw
   * @param tableName
   */
  public VertexIndexTableWrapper(AccumuloGraphConfiguration config,
      MultiTableBatchWriter mtbw) {
    super(config, mtbw, config.getVertexKeyIndexTableName());
  }

}
