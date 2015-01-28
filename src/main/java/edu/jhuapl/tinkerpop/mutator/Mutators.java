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
package edu.jhuapl.tinkerpop.mutator;

import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Range;

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.AccumuloGraphException;

public class Mutators {

  public static void apply(BatchWriter writer, Mutator mut) {
    try {
      writer.addMutations(mut.create());
    } catch (MutationsRejectedException e) {
      throw new AccumuloGraphException(e);
    }
  }

  public static void deleteElementRanges(BatchDeleter deleter, Element... elements) {
    List<Range> ranges = new LinkedList<Range>();

    for (Element element : elements) {
      ranges.add(new Range(element.getId().toString()));
    }
    deleter.setRanges(ranges);

    try {
      deleter.delete();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }
}
