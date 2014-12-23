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

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;

/**
 * Wrapper around index tables.
 */
public abstract class IndexTableWrapper extends BaseTableWrapper {

  /**
   * @param config
   * @param mtbw
   */
  protected IndexTableWrapper(AccumuloGraphConfiguration config,
      MultiTableBatchWriter mtbw, String tableName) {
    super(config, mtbw, tableName);
  }
}
