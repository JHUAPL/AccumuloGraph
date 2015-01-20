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
package edu.jhuapl.tinkerpop.tables;

import org.apache.accumulo.core.client.BatchWriter;
import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.AccumuloGraphUtils;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.index.IndexValueMutator;

/**
 * Wrapper around index tables.
 */
public abstract class IndexTableWrapper extends BaseTableWrapper {

  protected final Class<? extends Element> elementType;

  protected IndexTableWrapper(GlobalInstances globals,
      Class<? extends Element> elementType, String tableName) {
    super(globals, tableName);
    this.elementType = elementType;
  }

  /**
   * Add the property to this index.
   * 
   * <p/>Note that this requires a round-trip to Accumulo to see
   * if the property exists if the provided key has an index.
   * So for best performance, create indices after bulk ingest.
   * @param element
   * @param key
   * @param value
   */
  public void setPropertyForIndex(Element element, String key, Object value) {
    AccumuloGraphUtils.validateProperty(key, value);
    if (globals.getConfig().getAutoIndex() ||
        globals.getGraph().getIndexedKeys(elementType).contains(key)) {
      BatchWriter writer = getWriter();

      Object oldValue = element.getProperty(key);
      if (oldValue != null) {
        Mutators.apply(writer, new IndexValueMutator.Delete(element, key, oldValue));
      }

      Mutators.apply(writer, new IndexValueMutator.Add(element, key, value));

      globals.checkedFlush();
    }
  }

  /**
   * Remove property from the index.
   * @param element
   * @param key
   * @param value
   */
  public void removePropertyFromIndex(Element element, String key, Object value) {
    if (value != null) {
      Mutators.apply(getWriter(), new IndexValueMutator.Delete(element, key, value));
      globals.checkedFlush();
    }
  }
}
