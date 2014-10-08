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
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.util.StringFactory;

final class AccumuloGraphUtils {

  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  public static final Text EMPTY_TEXT = new Text("");

  public static final Text ID = new Text(StringFactory.ID);

  public static final Text IN = new Text("I");
  public static final Text OUT = new Text("O");

  public static final String toId(Object obj) {
    return obj.toString();
  }

  public static Value toValue(String val) {
    return new Value(val.getBytes());
  }

  /**
   * Create and/or clear existing graph tables for the given configuration.
   * 
   * @param cfg
   */
  static void handleCreateAndClear(AccumuloGraphConfiguration cfg) {
    try {
      TableOperations tableOps = cfg.getConnector().tableOperations();

      // Track whether tables existed before we do anything.
      boolean existedBeforeClear = false;
      for (String table : cfg.getTableNames()) {
        if (tableOps.exists(table)) {
          existedBeforeClear = true;
          break;
        }
      }

      // Check edge cases.
      // No tables exist, and we are not allowed to create.
      if (!existedBeforeClear && !cfg.isCreate()) {
        throw new IllegalArgumentException("Graph does not exist, and create option is disabled");
      }
      // Tables exist, and we are not clearing them.
      else if (existedBeforeClear && !cfg.isClear()) {
        // Do nothing.
        return;
      }

      // We want to clear tables, so do it.
      if (cfg.isClear()) {
        for (String table : cfg.getTableNames()) {
          if (tableOps.exists(table)) {
            tableOps.delete(table);
          }
        }
      }

      // Tables existed, or we want to create them. So do it.
      if (existedBeforeClear || cfg.isCreate()) {
        for (String table : cfg.getTableNames()) {
          if (!tableOps.exists(table)) {
            tableOps.create(table);
            SortedSet<Text> splits = cfg.getSplits();
            if (splits != null) {
              tableOps.addSplits(table, splits);
            }
          }
        }
      }

    } catch (AccumuloException e) {
      throw new IllegalArgumentException(e);
    } catch (AccumuloSecurityException e) {
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    } catch (InterruptedException e) {
      throw new IllegalArgumentException(e);
    } catch (TableNotFoundException e) {
      throw new IllegalArgumentException(e);
    } catch (TableExistsException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
