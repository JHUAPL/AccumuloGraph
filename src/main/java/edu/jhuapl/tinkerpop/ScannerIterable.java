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

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;

import com.tinkerpop.blueprints.Element;

/**
 * TODO
 */
public abstract class ScannerIterable<T extends Element> implements Iterable<T>, Closeable {

  private ScannerBase scanner;

  public ScannerIterable(ScannerBase scanner) {
    this.scanner = scanner;
  }

  @Override
  public Iterator<T> iterator() {
    return new ScannerIterator(new PeekingIterator<Entry<Key,Value>>(scanner.iterator()));
  }

  public abstract T next(PeekingIterator<Entry<Key,Value>> iterator);

  @Override
  public void close() {
    if (scanner != null) {
      scanner.close();
      scanner = null;
    }
  }

  @Override
  protected void finalize() {
    close();
  }

  class ScannerIterator implements Iterator<T> {
    PeekingIterator<Entry<Key,Value>> iterator;

    ScannerIterator(PeekingIterator<Entry<Key,Value>> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return ScannerIterable.this.next(iterator);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
