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
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloGraphUtils;
import edu.jhuapl.tinkerpop.Constants;
import edu.jhuapl.tinkerpop.GlobalInstances;

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
   * @param val
   */
  public void setPropertyForIndex(Element element, String key, Object val) {
    AccumuloGraphUtils.validateProperty(key, val);
    try {
      if (globals.getConfig().getAutoIndex() ||
          globals.getGraph().getIndexedKeys(elementType).contains(key)) {
        BatchWriter bw = getWriter();

        Object old = element.getProperty(key);
        if (old != null) {
          byte[] oldByteVal = AccumuloByteSerializer.serialize(old);
          Mutation m = new Mutation(oldByteVal);
          m.putDelete(key, element.getId().toString());
          bw.addMutation(m);
        }

        byte[] newByteVal = AccumuloByteSerializer.serialize(val);
        Mutation m = new Mutation(newByteVal);
        m.put(key.getBytes(), element.getId().toString().getBytes(), Constants.EMPTY);
        bw.addMutation(m);
        globals.checkedFlush();
      }
    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }
}
