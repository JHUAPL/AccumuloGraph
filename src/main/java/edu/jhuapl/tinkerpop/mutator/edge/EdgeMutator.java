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
package edu.jhuapl.tinkerpop.mutator.edge;

import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;

public final class EdgeMutator {

  public static class Add extends BaseEdgeMutator {

    public Add(Edge edge) {
      super(edge);
    }

    @Override
    public Iterable<Mutation> create() {
      Object inVertexId = edge.getVertex(Direction.IN).getId();
      Object outVertexId = edge.getVertex(Direction.OUT).getId();

      Mutation m = new Mutation(edge.getId().toString());
      m.put(AccumuloGraph.LABEL,
          (inVertexId + AccumuloGraph.IDDELIM + outVertexId).getBytes(),
          AccumuloByteSerializer.serialize(edge.getLabel()));

      return Lists.newArrayList(m);
    }
  }

  public static class Delete extends BaseEdgeMutator {

    public Delete(Edge edge) {
      super(edge);
    }

    @Override
    public Iterable<Mutation> create() {
      Object inVertexId = edge.getVertex(Direction.IN).getId();
      Object outVertexId = edge.getVertex(Direction.OUT).getId();

      Mutation m = new Mutation(edge.getId().toString());
      m.putDelete(AccumuloGraph.LABEL,
          (inVertexId + AccumuloGraph.IDDELIM + outVertexId).getBytes());

      return Lists.newArrayList(m);
    }
  }
}