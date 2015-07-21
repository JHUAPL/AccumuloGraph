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
package edu.jhuapl.tinkerpop.tables.index;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloElement;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloGraphUtils;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.ScannerIterable;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.index.IndexValueMutator;
import edu.jhuapl.tinkerpop.parser.EdgeIndexParser;
import edu.jhuapl.tinkerpop.parser.ElementIndexParser;
import edu.jhuapl.tinkerpop.parser.VertexIndexParser;
import edu.jhuapl.tinkerpop.tables.BaseTableWrapper;

/**
 * Wrapper around index tables containing properties
 * and values.
 */
public abstract class BaseIndexValuesTableWrapper extends BaseTableWrapper {

  protected final Class<? extends Element> elementType;

  protected BaseIndexValuesTableWrapper(GlobalInstances globals,
      Class<? extends Element> elementType, String tableName) {
    super(globals, tableName);
    this.elementType = elementType;
  }

  /**
   * Return class of this index.
   * @return
   */
  public Class<? extends Element> getElementType() {
    return elementType;
  }

  /**
   * Add the property to this index, if autoindexing is enabled
   * and/or the given key has indexing enabled.
   * @param element
   * @param key
   * @param value
   */
  public void setPropertyForIndex(Element element, String key, Object value) {
    setPropertyForIndex(element, key, value, false);
  }

  /**
   * Add the property to this index.
   * 
   * <br>Note that this requires a round-trip to Accumulo to see
   * if the property exists if the provided key has an index.
   * So for best performance, create indices after bulk ingest.
   * <br>If the force parameter is true, set the property regardless
   * of whether indexing is enabled for the given key. This is needed
   * for {@link IndexableGraph} operations.
   * @param element
   * @param key
   * @param value
   * @param force
   */
  public void setPropertyForIndex(Element element, String key, Object value,
      boolean force) {
    AccumuloGraphUtils.validateProperty(key, value);
    if (force || globals.getConfig().getAutoIndex() ||
        globals.getIndexMetadataWrapper()
        .getIndexedKeys(elementType).contains(key)) {
      BatchWriter writer = getWriter();

      Object oldValue = element.getProperty(key);
      if (oldValue != null && !oldValue.equals(value)) {
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

  /**
   * Get elements with the key/value pair.
   * @param key
   * @param value
   * @return
   */
  @SuppressWarnings("unchecked")
  public <T extends Element> CloseableIterable<T> readElementsFromIndex(String key, Object value) {
    Scanner scan = getScanner();
    byte[] id = AccumuloByteSerializer.serialize(value);
    scan.setRange(Range.exact(new Text(id)));
    scan.fetchColumnFamily(new Text(key));

    final ElementIndexParser<? extends AccumuloElement> parser =
        Vertex.class.equals(elementType) ? new VertexIndexParser(globals) :
          new EdgeIndexParser(globals);

        return new ScannerIterable<T>(scan) {
          @Override
          public T next(PeekingIterator<Entry<Key,Value>> iterator) {
            return (T) parser.parse(Arrays.asList(iterator.next()));
          }      
        };
  }

  /**
   * Remove the given element's properties from the index.
   * @param element
   */
  public void removeElementFromIndex(Element element) {
    BatchDeleter deleter = null;

    try {
      deleter = getDeleter();
      deleter.setRanges(Collections.singleton(new Range()));

      IteratorSetting is = new IteratorSetting(10, "getEdgeFilter", RegExFilter.class);
      RegExFilter.setRegexs(is, null, null,
          "^"+Pattern.quote(element.getId().toString())+"$", null, false);
      deleter.addScanIterator(is);
      deleter.delete();
      deleter.close();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (deleter != null) {
        deleter.close();
      }
    }
  }
}
