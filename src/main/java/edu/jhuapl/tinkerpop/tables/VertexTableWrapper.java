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
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphException;


/**
 * Wrapper around {@link Vertex} tables.
 */
public class VertexTableWrapper extends ElementTableWrapper {

  public VertexTableWrapper(AccumuloGraphConfiguration config,
      MultiTableBatchWriter writer) {
    super(config, writer, config.getVertexTable());
  }

  /**
   * Write a vertex with the given id.
   * Note: This does not currently write the vertex's properties.
   * @param id
   */
  public void writeVertex(String id) {
    Mutation m = new Mutation(id);
    m.put(AccumuloGraph.LABEL, AccumuloGraph.EXISTS, AccumuloGraph.EMPTY);
    try {
      getWriter().addMutation(m);
    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }

  /**
   * Write edge endpoint information to the vertex table.
   * @param id
   * @param outVertexId
   * @param inVertexId
   * @param label
   */
  public void writeEdgeEndpoints(String id, String outVertexId,
      String inVertexId, String label) {
    try {
      Mutation m = new Mutation(inVertexId);
      m.put(AccumuloGraph.INEDGE,
          (outVertexId + AccumuloGraph.IDDELIM + id).getBytes(),
          (AccumuloGraph.IDDELIM + label).getBytes());
      getWriter().addMutation(m);

      m = new Mutation(outVertexId);
      m.put(AccumuloGraph.OUTEDGE,
          (inVertexId + AccumuloGraph.IDDELIM + id).getBytes(),
          (AccumuloGraph.IDDELIM + label).getBytes());
      getWriter().addMutation(m);

    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }
}
