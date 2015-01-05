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
package edu.jhuapl.tinkerpop.parser;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloElement;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.mutator.property.PropertyUtils;

/**
 * TODO
 */
public abstract class ElementParser<T extends AccumuloElement> {

  protected GlobalInstances globals;

  public ElementParser(GlobalInstances globals) {
    this.globals = globals;
  }

  /**
   * Given the element id and set of entries,
   * create the type of element.  This can leverage
   * the property loader, etc.
   * @param id
   * @param entries
   * @return
   */
  public abstract T parse(String id, Iterable<Entry<Key, Value>> entries);

  /**
   * Parse out the property entries and set them for the given element.
   * @param element
   * @param entries
   */
  protected void setInMemoryProperties(T element, Iterable<Entry<Key, Value>> entries) {
    Map<String, Object> props = PropertyUtils.parseProperties(entries);
    for (String key : props.keySet()) {
      element.setPropertyInMemory(key, props.get(key));
    }
  }
}
