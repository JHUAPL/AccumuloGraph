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
package edu.jhuapl.tinkerpop.mutator;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;

import edu.jhuapl.tinkerpop.AccumuloGraphException;

public class Mutators {

  public static void apply(BatchWriter writer, BaseMutator mut) {
    try {
      writer.addMutations(mut.create());
    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }
}
