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

import java.util.SortedSet;
import java.util.UUID;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.io.Text;

final class AccumuloGraphUtils {

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
      if (!existedBeforeClear && !cfg.getCreate()) {
        throw new IllegalArgumentException("Graph does not exist, and create option is disabled");
      }
      // Tables exist, and we are not clearing them.
      else if (existedBeforeClear && !cfg.getClear()) {
        // Do nothing.
        return;
      }

      // We want to clear tables, so do it.
      if (cfg.getClear()) {
        for (String table : cfg.getTableNames()) {
          if (tableOps.exists(table)) {
            tableOps.delete(table);
          }
        }
      }

      // Tables existed, or we want to create them. So do it.
      if (existedBeforeClear || cfg.getCreate()) {
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

    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Generate an element id.
   * @return
   */
  public static String generateId() {
    return UUID.randomUUID().toString();
  }
}
