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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.TestSuite;

public class AccumuloGraphTestSuite extends TestSuite {

	public void testSplits() throws Exception {
		AccumuloGraphConfiguration cfg;

		// Tests for splits string.
		cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplits").splits((String) null);
		AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplits").splits("");
		graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("threeSplits").splits(" a b c ");
		graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			Collection<Text> splits = cfg.getConnector().tableOperations().listSplits(table);
			assertEquals(3, splits.size());
			List<Text> arr = new ArrayList<Text>(splits);
			assertEquals("a", arr.get(0).toString());
			assertEquals("b", arr.get(1).toString());
			assertEquals("c", arr.get(2).toString());
		}
		graph.shutdown();

		// Tests for splits array.
		cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplitsArray").splits((String[]) null);
		graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplitsArray").splits(new String[]{});
		graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("threeSplitsArray")
				.splits(new String[]{"d", "e", "f"});
		graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			Collection<Text> splits = cfg.getConnector().tableOperations().listSplits(table);
			assertEquals(3, splits.size());
			List<Text> arr = new ArrayList<Text>(splits);
			assertEquals("d", arr.get(0).toString());
			assertEquals("e", arr.get(1).toString());
			assertEquals("f", arr.get(2).toString());
		}
		graph.shutdown();
	}
}
