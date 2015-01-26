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
package edu.jhuapl.tinkerpop.tables.namedindex;

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.tables.BaseIndexedItemsListTableWrapper;

/**
 * Wrapper around index metadata table. This lists
 * names of indexes and their element types.
 */
public class NamedIndexListTableWrapper extends BaseIndexedItemsListTableWrapper {

  public NamedIndexListTableWrapper(GlobalInstances globals) {
    super(globals, globals.getConfig().getIndexNamesTableName());
  }

  public void writeIndexNameEntry(String indexName,
      Class<? extends Element> indexClass) {
    writeEntry(indexName, indexClass);
  }

  public void clearIndexNameEntry(String indexName,
      Class<? extends Element> indexClass) {
    clearEntry(indexName, indexClass);
  }
}
