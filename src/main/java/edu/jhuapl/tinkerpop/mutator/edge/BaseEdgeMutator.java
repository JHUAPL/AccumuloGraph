package edu.jhuapl.tinkerpop.mutator.edge;

import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.mutator.BaseMutator;

public abstract class BaseEdgeMutator extends BaseMutator {

  protected final Edge edge;

  public BaseEdgeMutator(Edge edge) {
    this.edge = edge;
  }
}
