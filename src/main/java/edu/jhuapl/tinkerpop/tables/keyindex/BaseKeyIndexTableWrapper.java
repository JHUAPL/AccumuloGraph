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
package edu.jhuapl.tinkerpop.tables.keyindex;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.tables.BaseIndexValuesTableWrapper;
import edu.jhuapl.tinkerpop.tables.core.EdgeTableWrapper;
import edu.jhuapl.tinkerpop.tables.core.ElementTableWrapper;
import edu.jhuapl.tinkerpop.tables.core.VertexTableWrapper;

/**
 * Base class for key index tables.
 */
public class BaseKeyIndexTableWrapper extends BaseIndexValuesTableWrapper {

  protected BaseKeyIndexTableWrapper(GlobalInstances globals,
      Class<? extends Element> elementType, String tableName) {
    super(globals, elementType, tableName);
  }

  /**
   * Rebuild this index for the given table.
   * @param table
   * @param key
   */
  public void rebuildIndex(String key, Class<? extends Element> elementClass) {
    ElementTableWrapper wrapper = globals.getElementWrapper(elementClass);
    if (wrapper instanceof VertexTableWrapper) {
      for (Vertex v : ((VertexTableWrapper) wrapper).getVertices()) {
        rebuild(wrapper, v, key);
      }
    }
    else if (wrapper instanceof EdgeTableWrapper) {
      for (Edge e : ((EdgeTableWrapper) wrapper).getEdges()) {
        rebuild(wrapper, e, key);
      }
    }
    else {
      throw new AccumuloGraphException("Unexpected table wrapper: "+wrapper.getClass());
    }
    globals.checkedFlush();
  }

  /**
   * Add given element to index for the given key.
   * @param element
   * @param key
   */
  private void rebuild(ElementTableWrapper wrapper,
      Element element, String key) {
    Object value = wrapper.readProperty(element, key);
    if (value != null) {
      setPropertyForIndex(element, key, value);
    }
  }
}
