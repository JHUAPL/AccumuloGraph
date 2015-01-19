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

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.Constants;
import edu.jhuapl.tinkerpop.mutator.Mutator;

/**
 * Mutators for index metadata table entries.
 */
public class IndexMetadataMutator {

  private IndexMetadataMutator() { }

  public static class Add implements Mutator {

    private final String key;
    private final Class<? extends Element> clazz;

    public Add(String key, Class<? extends Element> clazz) {
      this.key = key;
      this.clazz = clazz;
    }

    @Override
    public Iterable<Mutation> create() {
      Mutation m = new Mutation(key);
      m.put(clazz.getSimpleName().getBytes(),
          Constants.EMPTY, Constants.EMPTY);
      return Lists.newArrayList(m);
    }
  }

  public static class Delete implements Mutator {

    private final String key;
    private final Class<? extends Element> clazz;

    public Delete(String indexName, Class<? extends Element> clazz) {
      this.key = indexName;
      this.clazz = clazz;
    }

    @Override
    public Iterable<Mutation> create() {
      Mutation m = new Mutation(key);
      m.putDelete(clazz.getSimpleName().getBytes(), Constants.EMPTY);
      return Lists.newArrayList(m);
    }
  }
}
