package edu.jhuapl.tinkerpop;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

public class AccumuloProperty<V> implements Property<V>{

  final AccumuloElement element;
  final String key;
  final V value;
  
  public AccumuloProperty(AccumuloElement ele, String key, V value){
    this.element=ele;
    this.key=key;
    this.value=value;
  }
  
  @Override
  public String key() {
    return key;
  }

  @Override
  public V value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return value!=null;
  }

  @Override
  public Element element() {
    return element;
  }

  @Override
  public void remove() {
    element.removeProperty(key);
  }

}
