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
package edu.jhuapl.tinkerpop;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;


public class AccumuloIndex<T extends Element> implements Index<T> {
  Class indexedType;
  AccumuloGraph parent;
  String indexName;
  String tableName;

  public AccumuloIndex(Class t, AccumuloGraph parent, String indexName) {
    indexedType = t;
    this.parent = parent;
    this.indexName = indexName;
    tableName = parent.config.getGraphName() + "_index_" + indexName;// + "_" +
    // t;

    try {
      if (!parent.config.getConnector().tableOperations().exists(tableName)) {
        parent.config.getConnector().tableOperations().create(tableName);
      }
    } catch (Exception e) {
     throw new RuntimeException(e);
    }

  }

  public String getIndexName() {
    return indexName;
  }



  public void put(String key, Object value, Element element) {
    element.setProperty(key, value);
    Mutation m = new Mutation(AccumuloByteSerializer.serialize(value));
    m.put(key.getBytes(), element.getId().toString().getBytes(), "".getBytes());
    BatchWriter w = getWriter();
    try {
      w.addMutation(m);
      w.flush();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }

  }

  public CloseableIterable<T> get(String key, Object value) {
    Scanner scan = getScanner();
    byte[] id = AccumuloByteSerializer.serialize(value);
    scan.setRange(new Range(new Text(id), new Text(id)));
    scan.fetchColumnFamily(new Text(key));

    return new IndexIterable(parent, scan, indexedType);
  }

  public CloseableIterable<T> query(String key, Object query) {
    throw new UnsupportedOperationException();

  }

  public long count(String key, Object value) {
    CloseableIterable<T> iterable = get(key, value);
    Iterator<T> iter = iterable.iterator();
    int count = 0;
    while (iter.hasNext()) {
      count++;
      iter.next();
    }
    iterable.close();
    return count;
  }

  public void remove(String key, Object value, Element element) {
    Mutation m = new Mutation(AccumuloByteSerializer.serialize(value));
    m.putDelete(key.getBytes(), element.getId().toString().getBytes());
    BatchWriter w = getWriter();
    try {
      w.addMutation(m);
      w.flush();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }

  }

  private BatchWriter getWriter() {
    return parent.getWriter(tableName);
  }

  private Scanner getScanner() {
    return parent.getScanner(tableName);
  }

  public class IndexIterable implements CloseableIterable<T> {
    AccumuloGraph parent;
    ScannerBase scan;
    boolean isClosed;
    Class indexedType;

    IndexIterable(AccumuloGraph parent, ScannerBase scan, Class t) {
      this.scan = scan;
      this.parent = parent;
      isClosed = false;
      indexedType = t;
    }

    public Iterator<T> iterator() {
      if (!isClosed) {
        if(indexedType.equals(Edge.class)){
          
            return new ScannerIterable<T>(parent, scan) {

              @Override
              public T next(PeekingIterator<Entry<Key,Value>> iterator) {
                // TODO better use of information readily
                // available...
                return (T) new AccumuloEdge(parent, iterator.next().getKey().getColumnQualifier().toString());
              }
            }.iterator();
        }else{ 
            return new ScannerIterable<T>(parent, scan) {

              @Override
              public T next(PeekingIterator<Entry<Key,Value>> iterator) {
                // TODO better use of information readily
                // available...
                return (T) new AccumuloVertex(parent, iterator.next().getKey().getColumnQualifier().toString());
              }
            }.iterator();
        }
      }
      return null;
    }

    public void close() {
      if (!isClosed) {
        scan.close();
        isClosed = true;
      }
    }

  }

  @Override
  public Class<T> getIndexClass() {
    return indexedType;
  }

}
