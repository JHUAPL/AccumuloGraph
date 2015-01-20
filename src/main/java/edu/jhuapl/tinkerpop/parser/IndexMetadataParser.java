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

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.tinkerpop.blueprints.Element;

/**
 * Entry parser for index metadata. This includes index
 * names for named indexes, and property keys for
 * key indexes.
 */
public class IndexMetadataParser implements EntryParser<List<String>> {

  private final Class<? extends Element> elementClass;

  public IndexMetadataParser(Class<? extends Element> elementClass) {
    this.elementClass = elementClass;
  }

  @Override
  public List<String> parse(Iterable<Entry<Key,Value>> entries) {
    List<String> keys = new ArrayList<String>();

    for (Entry<Key, Value> entry : entries) {
      String clazz = entry.getKey().getColumnFamily().toString();
      if (elementClass.getSimpleName().equals(clazz)) {
        keys.add(entry.getKey().getRow().toString());
      }
    }

    return keys;
  }
}
