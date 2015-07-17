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

import org.apache.tinkerpop.gremlin.structure.Element;

import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;

/**
 * An indexed item. For {@link IndexableGraph},
 * the key is the index name. For {@link KeyIndexableGraph}
 * the key is the indexed key.
 * @author Michael Lieberman
 *
 */
public class IndexedItem {
  private final String key;
  private final Class<? extends Element> elementClass;

  public IndexedItem(String key, Class<? extends Element> elementClass) {
    this.key = key;
    this.elementClass = elementClass;
  }

  public String getKey() {
    return key;
  }

  public Class<? extends Element> getElementClass() {
    return elementClass;
  }
}
