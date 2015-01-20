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
import java.util.Set;
import org.apache.accumulo.core.client.Scanner;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.parser.IndexMetadataParser;

/**
 * Wraps the metadata tables which stores information
 * about which property keys are indexed for different
 * graph types.
 */
public class IndexedKeysTableWrapper extends MetadataTableWrapper {

  public IndexedKeysTableWrapper(GlobalInstances globals) {
    super(globals, globals.getConfig().getIndexedKeysTableName());
  }

  public void writeKeyMetadataEntry(String key, Class<? extends Element> clazz) {
    writeEntry(key, clazz);
  }

  public void clearKeyMetadataEntry(String key, Class<? extends Element> clazz) {
    clearEntry(key, clazz);
  }

  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }

    IndexMetadataParser parser = new IndexMetadataParser(elementClass);

    Scanner s = getScanner();
    Set<String> keys = new HashSet<String>(parser.parse(s));
    s.close();

    return keys;
  }
}
