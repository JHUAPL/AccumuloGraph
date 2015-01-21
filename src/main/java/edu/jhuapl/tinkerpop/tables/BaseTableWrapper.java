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

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.GlobalInstances;

/**
 * Table wrapper with common functionality.
 */
public abstract class BaseTableWrapper {

  protected GlobalInstances globals;
  private String tableName;

  public BaseTableWrapper(GlobalInstances globals, String tableName) {
    this.globals = globals;
    this.tableName = tableName;
  }

  protected Scanner getScanner() {
    try {
      return globals.getConfig().getConnector().createScanner(tableName,
          globals.getConfig().getAuthorizations());

    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  protected BatchScanner getBatchScanner() {
    try {
      BatchScanner scanner = globals.getConfig().getConnector().createBatchScanner(tableName,
          globals.getConfig().getAuthorizations(), globals.getConfig().getQueryThreads());
      scanner.setRanges(Collections.singletonList(new Range()));
      return scanner;
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  protected BatchWriter getWriter() {
    try {
      return globals.getMtbw().getBatchWriter(tableName);
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  protected BatchDeleter getDeleter() {
    try {
      return globals.getConfig().getConnector().createBatchDeleter(tableName,
          globals.getConfig().getAuthorizations(), globals.getConfig().getMaxWriteThreads(),
          globals.getConfig().getBatchWriterConfig());
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  public void dump() {
    System.out.println("Dump of table "+tableName+":");
    Scanner s = getScanner();
    for (Entry<Key, Value> entry : s) {
      System.out.println("  "+entry);
    }
  }
}
