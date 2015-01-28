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
package edu.jhuapl.tinkerpop;

import java.util.Iterator;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;

import edu.jhuapl.tinkerpop.tables.namedindex.NamedIndexTableWrapper;

/**
 * Accumulo-based implementation for {@link IndexableGraph}.
 * @param <T>
 */
public class AccumuloIndex<T extends Element> implements Index<T> {
  private final GlobalInstances globals;
  private final Class<T> indexedType;
  private final String indexName;
  private final NamedIndexTableWrapper indexWrapper;

  public AccumuloIndex(GlobalInstances globals, String indexName, Class<T> indexedType) {
    this.globals = globals;
    this.indexName = indexName;
    this.indexedType = indexedType;

    try {
      if (!globals.getConfig().getConnector()
          .tableOperations().exists(getTableName())) {
        globals.getConfig().getConnector()
        .tableOperations().create(getTableName());
      }
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }

    indexWrapper = new NamedIndexTableWrapper(globals, indexedType, indexName);
  }

  @Override
  public String getIndexName() {
    return indexName;
  }

  public String getTableName() {
    return globals.getConfig().getNamedIndexTableName(indexName);
  }

  public NamedIndexTableWrapper getWrapper() {
    return indexWrapper;
  }

  @Override
  public Class<T> getIndexClass() {
    return indexedType;
  }

  @Override
  public void put(String key, Object value, Element element) {
    indexWrapper.setPropertyForIndex(element, key, value, true);
  }

  @Override
  public CloseableIterable<T> get(String key, Object value) {
    return indexWrapper.readElementsFromIndex(key, value);
  }

  @Override
  public CloseableIterable<T> query(String key, Object query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long count(String key, Object value) {
    CloseableIterable<T> iterable = get(key, value);
    Iterator<T> iter = iterable.iterator();
    int count = 0;
    while (iter.hasNext()) {
      count++;
      iter.next();
    }
    iterable.close();
    return count;
  }

  @Override
  public void remove(String key, Object value, Element element) {
    indexWrapper.removePropertyFromIndex(element, key, value);
  }
}
