/* Copyright 2014 The Johns Hopkins University Applied Physics Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.jhuapl.tinkerpop.parser;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloElement;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * TODO
 */
public abstract class ElementParser<T extends AccumuloElement> implements EntryParser<T> {

  protected GlobalInstances globals;

  public ElementParser(GlobalInstances globals) {
    this.globals = globals;
  }

  @Override
  public T parse(Iterable<Entry<Key, Value>> entries) {
    String id = entries.iterator().next().getKey().getRow().toString();
    return parse(id, entries);
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
    Map<String, Object> props = new PropertyParser().parse(entries);
    for (String key : props.keySet()) {
      element.setPropertyInMemory(key, props.get(key));
    }
  }
}
