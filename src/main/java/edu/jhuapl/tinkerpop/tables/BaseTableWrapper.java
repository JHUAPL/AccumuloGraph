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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * Table wrapper with common functionality.
 */
public abstract class BaseTableWrapper {

  protected GlobalInstances globals;
  private String tableName;

  public BaseTableWrapper(GlobalInstances globals, String tableName) {
    this.globals = globals;
    this.tableName = tableName;
  }

  protected Scanner getScanner() {
    try {
      return globals.getConfig().getConnector().createScanner(tableName,
          globals.getConfig().getAuthorizations());

    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  protected BatchWriter getWriter() {
    try {
      return globals.getMtbw().getBatchWriter(tableName);
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  public void dump() {
    System.out.println("Dump of table "+tableName+":");
    Scanner s = getScanner();
    for (Entry<Key, Value> entry : s) {
      System.out.println("  "+entry);
    }
  }
}
