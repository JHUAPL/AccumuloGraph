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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.Constants;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.mutator.property.ClearPropertyMutator;
import edu.jhuapl.tinkerpop.mutator.property.WritePropertyMutator;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.parser.PropertyParser;

/**
 * Wrapper around tables with operations
 * common to {@link Element}s.
 */
public abstract class ElementTableWrapper extends BaseTableWrapper {

  private BatchWriter writer;

  public ElementTableWrapper(GlobalInstances globals, String tableName) {
    super(globals, tableName);
    writer = super.getWriter();
  }

  /**
   * Give a single instance of the writer for this table.
   */
  @Override
  protected BatchWriter getWriter() {
    return writer;
  }

  /**
   * Read the given property from the backing table
   * for the given element id.
   * @param id
   * @param key
   * @return
   */
  public <V> V readProperty(Element element, String key) {
    Scanner s = getScanner();

    s.setRange(new Range(element.getId().toString()));

    Text colf = StringFactory.LABEL.equals(key)
        ? new Text(Constants.LABEL) : new Text(key);
    s.fetchColumnFamily(colf);

    V value = null;

    Iterator<Entry<Key, Value>> iter = s.iterator();
    if (iter.hasNext()) {
      value = AccumuloByteSerializer.deserialize(iter.next().getValue().get());
    }
    s.close();

    return value;
  }

  /**
   * Read the given properties for the given element id.
   * This may return an empty map for elements with no properties.
   * If the element does not exist, return null.
   * @param id
   * @param propertyKeys
   * @return
   */
  public Map<String, Object> readProperties(Element element, String... propertyKeys) {
    Scanner s = getScanner();
    s.setRange(new Range(element.getId().toString()));
    s.fetchColumnFamily(new Text(Constants.LABEL));

    for (String key : propertyKeys) {
      s.fetchColumnFamily(new Text(key));
    }

    Map<String, Object> props = new PropertyParser().parse(s);
    s.close();

    return props;
  }

  /**
   * Get all property keys for the given element id.
   * @param id
   * @return
   */
  public Set<String> readPropertyKeys(Element element) {
    Scanner s = getScanner();

    s.setRange(new Range(element.getId().toString()));

    Set<String> keys = new HashSet<String>();

    for (Entry<Key, Value> entry : s) {
      String cf = entry.getKey().getColumnFamily().toString();
      keys.add(cf);
    }

    s.close();

    // Remove some special keys.
    keys.remove(Constants.IN_EDGE);
    keys.remove(Constants.LABEL);
    keys.remove(Constants.OUT_EDGE);

    return keys;
  }

  /**
   * Delete the property entry from property table.
   * @param id
   * @param key
   */
  public void clearProperty(Element element, String key) {
    Mutators.apply(getWriter(), new ClearPropertyMutator(element, key));
    globals.checkedFlush();
  }

  /**
   * Write the given property to the property table.
   * @param id
   * @param key
   * @param value
   */
  public void writeProperty(Element element, String key, Object value) {
    Mutators.apply(getWriter(),
        new WritePropertyMutator(element, key, value));
    globals.checkedFlush();
  }

  /**
   * Add custom iterator to the given scanner so that
   * it will only return keys with value corresponding to an edge.
   * @param scan
   * @param labels
   */
  protected void applyEdgeLabelValueFilter(Scanner scan, String... labels) {
    StringBuilder regex = new StringBuilder();
    for (String lab : labels) {
      if (regex.length() != 0)
        regex.append("|");
      regex.append(".*"+Constants.ID_DELIM+"\\Q").append(lab).append("\\E$");
    }

    IteratorSetting is = new IteratorSetting(10, "edgeValueFilter", RegExFilter.class);
    RegExFilter.setRegexs(is, null, null, null, regex.toString(), false);
    scan.addScanIterator(is);
  }

  public void close() {
    // TODO?
  }

  /**
   * Ensure that the given key/value don't conflict with
   * Blueprints reserved words.
   * @param key
   * @param value
   */
  protected void validateProperty(String key, Object value) {
    nullCheckProperty(key, value);
    if (key.equals(StringFactory.ID)) {
      throw ExceptionFactory.propertyKeyIdIsReserved();
    } else if (key.equals(StringFactory.LABEL)) {
      throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
    } else if (value == null) {
      throw ExceptionFactory.propertyValueCanNotBeNull();
    }
  }

  /**
   * Disallow null keys/values and throw appropriate
   * Blueprints exceptions.
   * @param key
   * @param value
   */
  protected void nullCheckProperty(String key, Object value) {
    if (key == null) {
      throw ExceptionFactory.propertyKeyCanNotBeNull();
    } else if (value == null) {
      throw ExceptionFactory.propertyValueCanNotBeNull();
    } else if (key.trim().equals(StringFactory.EMPTY_STRING)) {
      throw ExceptionFactory.propertyKeyCanNotBeEmpty();
    }
  }
}
