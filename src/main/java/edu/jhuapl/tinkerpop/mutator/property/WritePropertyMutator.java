package edu.jhuapl.tinkerpop.mutator.property;

import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;

public class WritePropertyMutator extends BasePropertyMutator {

  private Object value;

  public WritePropertyMutator(Element element, String key, Object value) {
    super(element, key);
    this.value = value;
  }

  @Override
  public Iterable<Mutation> create() {
    byte[] bytes = AccumuloByteSerializer.serialize(value);
    Mutation m = new Mutation(element.getId().toString());
    m.put(key.getBytes(), AccumuloGraph.EMPTY, bytes);
    return Lists.newArrayList(m);
  }
}
