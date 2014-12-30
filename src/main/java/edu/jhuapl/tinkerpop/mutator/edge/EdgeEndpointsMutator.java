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

import edu.jhuapl.tinkerpop.AccumuloGraph;

public class EdgeEndpointsMutator {

  private EdgeEndpointsMutator() {
  }

  public static class Add extends BaseEdgeMutator {

    public Add(Edge edge) {
      super(edge);
    }

    @Override
    public Iterable<Mutation> create() {
      String inVertexId = edge.getVertex(Direction.IN).getId().toString();
      String outVertexId = edge.getVertex(Direction.OUT).getId().toString();

      Mutation in = new Mutation(inVertexId);
      in.put(AccumuloGraph.INEDGE,
          (outVertexId + AccumuloGraph.IDDELIM + edge.getId()).getBytes(),
          (AccumuloGraph.IDDELIM + edge.getLabel()).getBytes());

      Mutation out = new Mutation(outVertexId);
      out.put(AccumuloGraph.OUTEDGE,
          (inVertexId + AccumuloGraph.IDDELIM + edge.getId()).getBytes(),
          (AccumuloGraph.IDDELIM + edge.getLabel()).getBytes());

      return Lists.newArrayList(in, out);
    }
  }

  public static class Delete extends BaseEdgeMutator {

    public Delete(Edge edge) {
      super(edge);
    }

    @Override
    public Iterable<Mutation> create() {
      String inVertexId = edge.getVertex(Direction.IN).getId().toString();
      String outVertexId = edge.getVertex(Direction.OUT).getId().toString();

      Mutation in = new Mutation(inVertexId);
      in.putDelete(AccumuloGraph.INEDGE,
          (outVertexId + AccumuloGraph.IDDELIM + edge.getId()).getBytes());

      Mutation out = new Mutation(outVertexId);
      out.putDelete(AccumuloGraph.OUTEDGE,
          (inVertexId + AccumuloGraph.IDDELIM + edge.getId()).getBytes());

      return Lists.newArrayList(in, out);
    }
  }
}
