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
package edu.jhuapl.tinkerpop.mutator.vertex;

import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.mutator.BaseMutator;

public abstract class BaseVertexMutator extends BaseMutator {

  protected final Vertex vertex;

  public BaseVertexMutator(Vertex vertex) {
    this.vertex = vertex;
  }
}
