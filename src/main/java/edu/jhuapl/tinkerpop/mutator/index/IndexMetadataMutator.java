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
package edu.jhuapl.tinkerpop.mutator.index;

import org.apache.accumulo.core.data.Mutation;
import org.apache.tinkerpop.gremlin.structure.Element;

import com.google.common.collect.Lists;

import edu.jhuapl.tinkerpop.Constants;
import edu.jhuapl.tinkerpop.Constants.IndexMetadataEntryType;
import edu.jhuapl.tinkerpop.mutator.Mutator;

/**
 * Mutators for index metadata table entries.
 */
public class IndexMetadataMutator {

  private IndexMetadataMutator() { }

  public static class Add implements Mutator {

    private final String key;
    private final Class<? extends Element> elementClass;
    private final IndexMetadataEntryType entryType;

    public Add(String key, Class<? extends Element> elementClass,
        IndexMetadataEntryType entryType) {
      this.key = key;
      this.elementClass = elementClass;
      this.entryType = entryType;
    }

    @Override
    public Iterable<Mutation> create() {
      Mutation m = new Mutation(key);
      m.put(entryType.name().getBytes(),
          elementClass.getName().getBytes(), Constants.EMPTY);
      return Lists.newArrayList(m);
    }
  }

  public static class Delete implements Mutator {

    private final String key;
    private final Class<? extends Element> elementClass;
    private final IndexMetadataEntryType entryType;

    public Delete(String indexName, Class<? extends Element> elementClass,
        IndexMetadataEntryType entryType) {
      this.key = indexName;
      this.elementClass = elementClass;
      this.entryType = entryType;
    }

    @Override
    public Iterable<Mutation> create() {
      Mutation m = new Mutation(key);
      m.putDelete(entryType.name().getBytes(),
          elementClass.getName().getBytes());
      return Lists.newArrayList(m);
    }
  }
}
