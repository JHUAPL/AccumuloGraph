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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloElement;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * Parser for elements based on an index table.
 */
public abstract class ElementIndexParser<T extends AccumuloElement>
    implements EntryParser<T> {

  protected final GlobalInstances globals;

  public ElementIndexParser(GlobalInstances globals) {
    this.globals = globals;
  }

  @Override
  public T parse(Iterable<Entry<Key, Value>> entries) {
    Iterator<Entry<Key, Value>> it = entries.iterator();

    if (it.hasNext()) {
      Entry<Key, Value> entry = it.next();

      if (it.hasNext()) {
        throw new AccumuloGraphException("Unexpected multiple entries for index table");
      }

      String id = entry.getKey().getColumnQualifierData().toString();
      T element = instantiate(id);

      // While we're here, read the property key/value.
      String key = entry.getKey().getColumnFamily().toString();
      Object value = AccumuloByteSerializer.deserialize(entry.getKey().getRow().getBytes());
      element.setPropertyInMemory(key, value);

      return element;
    }
    else {
      throw new AccumuloGraphException("No index table entries found");
    }
  }
  
  /**
   * Instantiate an object to be returned.
   * @param id
   * @return
   */
  protected abstract T instantiate(String id);
}
