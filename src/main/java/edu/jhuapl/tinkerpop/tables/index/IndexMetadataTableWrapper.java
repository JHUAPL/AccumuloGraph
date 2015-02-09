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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.util.ExceptionFactory;

import edu.jhuapl.tinkerpop.AccumuloIndex;
import edu.jhuapl.tinkerpop.Constants.IndexMetadataEntryType;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.index.IndexMetadataMutator;
import edu.jhuapl.tinkerpop.parser.IndexedItem;
import edu.jhuapl.tinkerpop.parser.IndexedItemsListParser;
import edu.jhuapl.tinkerpop.tables.BaseTableWrapper;

/**
 * Stores metadata, in particular the indexed keys
 * for {@link KeyIndexableGraph}, and the list of
 * named indexes for {@link IndexableGraph}.
 */
public class IndexMetadataTableWrapper extends BaseTableWrapper {

  public IndexMetadataTableWrapper(GlobalInstances globals) {
    super(globals, globals.getConfig().getIndexMetadataTableName());
  }


  //////// Methods for KeyIndexableGraph ////////

  public void writeKeyMetadataEntry(String key, Class<? extends Element> clazz) {
    Mutators.apply(getWriter(), new IndexMetadataMutator.Add(key, clazz,
        IndexMetadataEntryType.__INDEX_KEY__));
  }

  public void clearKeyMetadataEntry(String key, Class<? extends Element> clazz) {
    Mutators.apply(getWriter(), new IndexMetadataMutator.Delete(key, clazz,
        IndexMetadataEntryType.__INDEX_KEY__));
  }

  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }

    IndexedItemsListParser parser = new IndexedItemsListParser(elementClass);

    Scanner scan = null;
    try {
      scan = getScanner();
      scan.fetchColumnFamily(new Text(IndexMetadataEntryType.__INDEX_KEY__.name()));

      Set<String> keys = new HashSet<String>();
      for (IndexedItem item : parser.parse(scan)) {
        keys.add(item.getKey());
      }

      return keys;

    } finally {
      if (scan != null) {
        scan.close();
      }
    }
  }


  //////// Methods for IndexableGraph ////////

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Iterable<Index<? extends Element>> getIndices() {
    List<Index<? extends Element>> indexes = new ArrayList<Index<? extends Element>>();

    IndexedItemsListParser parser = new IndexedItemsListParser();

    Scanner scan = null;
    try {
      scan = getScanner();
      scan.fetchColumnFamily(new Text(IndexMetadataEntryType.__INDEX_NAME__.name()));

      for (IndexedItem item : parser.parse(scan)) {
        indexes.add(new AccumuloIndex(globals,
            item.getKey(), item.getElementClass()));
      }

      return indexes;

    } finally {
      if (scan != null) {
        scan.close();
      }
    }
  }

  public <T extends Element> Index<T> getIndex(String indexName,
      Class<T> indexClass) {
    IndexedItemsListParser parser = new IndexedItemsListParser();

    Scanner scan = null;
    try {
      scan = getScanner();
      scan.fetchColumnFamily(new Text(IndexMetadataEntryType.__INDEX_NAME__.name()));

      for (IndexedItem item : parser.parse(scan)) {
        if (item.getKey().equals(indexName)) {
          if (item.getElementClass().equals(indexClass)) {
            return new AccumuloIndex<T>(globals, indexName,
                indexClass);
          }
          else {
            throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
          }
        }
      }
      return null;

    } finally {
      if (scan != null) {
        scan.close();
      }
    }
  }
  
  public <T extends Element> Index<T> createIndex(String indexName,
      Class<T> indexClass) {
    for (Index<?> index : getIndices()) {
      if (index.getIndexName().equals(indexName)) {
        throw ExceptionFactory.indexAlreadyExists(indexName);
      }
    }

    writeIndexNameEntry(indexName, indexClass);
    return new AccumuloIndex<T>(globals, indexName, indexClass);
  }

  private void writeIndexNameEntry(String indexName,
      Class<? extends Element> indexClass) {
    Mutators.apply(getWriter(), new IndexMetadataMutator.Add(indexName,
        indexClass, IndexMetadataEntryType.__INDEX_NAME__));
  }

  public void clearIndexNameEntry(String indexName,
      Class<? extends Element> indexClass) {
    Mutators.apply(getWriter(), new IndexMetadataMutator.Delete(indexName,
        indexClass, IndexMetadataEntryType.__INDEX_NAME__));
  }
}
