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
package edu.jhuapl.tinkerpop.mutator.property;

import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.Lists;
import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.Constants;

public class WritePropertyMutator extends BasePropertyMutator {

  private final Object value;

  public WritePropertyMutator(String id, String key, Object value) {
    super(id, key);
    this.value = value;
  }

  @Override
  public Iterable<Mutation> create() {
    byte[] bytes = AccumuloByteSerializer.serialize(value);
    Mutation m = new Mutation(id);
    m.put(key.getBytes(), Constants.EMPTY, bytes);
    return Lists.newArrayList(m);
  }
}
