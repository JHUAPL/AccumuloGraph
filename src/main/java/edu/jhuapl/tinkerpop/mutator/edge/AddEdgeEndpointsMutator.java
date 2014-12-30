package edu.jhuapl.tinkerpop.mutator.edge;

import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.AccumuloGraph;

public class AddEdgeEndpointsMutator extends BaseEdgeMutator {

  public AddEdgeEndpointsMutator(Edge edge) {
    super(edge);
  }

  @Override
  public Iterable<Mutation> create() {
    String inVertexId = edge.getVertex(Direction.IN).getId().toString();
    String outVertexId = edge.getVertex(Direction.OUT).getId().toString();

    Mutation mIn = new Mutation(inVertexId);
    mIn.put(AccumuloGraph.INEDGE,
        (outVertexId + AccumuloGraph.IDDELIM + edge.getId()).getBytes(),
        (AccumuloGraph.IDDELIM + edge.getLabel()).getBytes());

    Mutation mOut = new Mutation(outVertexId);
    mOut.put(AccumuloGraph.OUTEDGE,
        (inVertexId + AccumuloGraph.IDDELIM + edge.getId()).getBytes(),
        (AccumuloGraph.IDDELIM + edge.getLabel()).getBytes());

    return Lists.newArrayList(mIn, mOut);
  }
}
