package edu.jhuapl.tinkerpop.mutator.property;

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.mutator.BaseMutator;

public abstract class BasePropertyMutator extends BaseMutator {

  protected Element element;
  protected String key;

  public BasePropertyMutator(Element element, String key) {
    this.element = element;
    this.key = key;
  }
}
