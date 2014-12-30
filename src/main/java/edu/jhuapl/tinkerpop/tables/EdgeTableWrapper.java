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

import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.edge.EdgeMutator;


/**
 * Wrapper around {@link Edge} tables.
 */
public class EdgeTableWrapper extends ElementTableWrapper {

  public EdgeTableWrapper(GlobalInstances globals) {
    super(globals, globals.getConfig().getEdgeTableName());
  }

  /**
   * Write the given edge to the edge table. Does not
   * currently write the edge's properties.
   * 
   * <p/>Note: This only adds the edge information. Vertex
   * endpoint information needs to be written to the vertex
   * table via {@link VertexTableWrapper}.
   * @param edge
   */
  public void writeEdge(Edge edge) {
    Mutators.apply(getWriter(), new EdgeMutator.Add(edge));
  }

  public void deleteEdge(Edge edge) {
    Mutators.apply(getWriter(), new EdgeMutator.Delete(edge));
  }
}
