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

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.GlobalInstances;


/**
 * Wrapper around {@link Edge} tables.
 */
public class EdgeTableWrapper extends ElementTableWrapper {

  public EdgeTableWrapper(GlobalInstances globals) {
    super(globals.getConfig(), globals.getMtbw(),
        globals.getConfig().getEdgeTableName());
  }

  /**
   * Write the given edge to the edge table. Does not
   * currently write the edge's properties.
   * 
   * <p/>Note: This only adds the edge information. Vertex
   * endpoint information needs to be written to the vertex
   * table via {@link VertexTableWrapper}.
   * @param id
   * @param outVertex
   * @param inVertex
   * @param label
   */
  public void writeEdge(String id, String outVertexId,
      String inVertexId, String label) {
    try {
      Mutation m = new Mutation(id);

      String cq = inVertexId + AccumuloGraph.IDDELIM + outVertexId;
      m.put(AccumuloGraph.LABEL, cq.getBytes(),
          AccumuloByteSerializer.serialize(label));
      getWriter().addMutation(m);
    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }
}
