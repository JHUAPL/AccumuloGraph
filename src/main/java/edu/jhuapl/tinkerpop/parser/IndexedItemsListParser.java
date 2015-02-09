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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphException;

/**
 * Entry parser for index metadata. The format
 * is the same for both {@link IndexableGraph}
 * and {@link KeyIndexableGraph} functionality.
 * For the former, this parser returns names of
 * indexes. For the latter, the parser returns
 * indexed keys.
 */
public class IndexedItemsListParser implements EntryParser<List<IndexedItem>> {

  private final Class<? extends Element> elementClass;

  /**
   * Constructor to return all items regardless
   * of element class.
   */
  public IndexedItemsListParser() {
    this(Element.class);
  }

  /**
   * Create a parser for items of the specified element class.
   * This may be Vertex, Edge, or Element.
   * @param elementClass
   */
  public IndexedItemsListParser(Class<? extends Element> elementClass) {
    // Validate element class.
    if (!Vertex.class.equals(elementClass) &&
        !Edge.class.equals(elementClass) &&
        !Element.class.equals(elementClass)) {
      throw new IllegalArgumentException("elementClass must be Vertex, Edge or Element");
    }
    this.elementClass = elementClass;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IndexedItem> parse(Iterable<Entry<Key,Value>> entries) {
    List<IndexedItem> items = new ArrayList<IndexedItem>();

    for (Entry<Key, Value> entry : entries) {
      Class<? extends Element> clazz;
      try {
        clazz = (Class<? extends Element>) Class.forName(entry.getKey()
            .getColumnQualifier().toString());
      } catch (ClassNotFoundException e) {
        throw new AccumuloGraphException(e);
      }

      if (Element.class.equals(elementClass) ||
          elementClass.equals(clazz)) {
        IndexedItem item = new IndexedItem(entry.getKey()
            .getRow().toString(), clazz);
        items.add(item);
      }
    }

    return items;
  }
}
