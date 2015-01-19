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

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.index.IndexMetadataMutator;

/**
 * Wrapper around index metadata table.
 */
public class IndexMetadataTableWrapper extends BaseTableWrapper {

  public IndexMetadataTableWrapper(GlobalInstances globals,
      String indexName) {
    super(globals, globals.getConfig().getIndexTableName(indexName));
  }

  public void writeIndexMetadataEntry(String indexName,
      Class<? extends Element> clazz) {
    Mutators.apply(getWriter(), new IndexMetadataMutator.Add(indexName, clazz));
  }

  public void clearIndexMetadataEntry(String indexName,
      Class<? extends Element> clazz) {
    Mutators.apply(getWriter(), new IndexMetadataMutator.Delete(indexName, clazz));    
  }
}
