package edu.jhuapl.tinkerpop.mutator.property;

import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.AccumuloGraph;

public class ClearPropertyMutator extends BasePropertyMutator {

  public ClearPropertyMutator(Element element, String key) {
    super(element, key);
  }

  @Override
  public Iterable<Mutation> create() {
    Mutation m = new Mutation(element.getId().toString());
    m.putDelete(key.getBytes(), AccumuloGraph.EMPTY);
    return Lists.newArrayList(m);
  }
}
