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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.namespace.QName;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.Vertex;

public class AccumuloGraphConfigurationTest {
	
	@Test
	public void testSplits() throws Exception {
		AccumuloGraphConfiguration cfg;

		// Tests for splits string.
		cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplits").setSplits((String) null);
		AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplits").setSplits("");
		graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("threeSplits").setSplits(" a b c ");
		graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
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
		cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplitsArray").setSplits((String[]) null);
		graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplitsArray").setSplits(new String[]{});
		graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = AccumuloGraphTestUtils.generateGraphConfig("threeSplitsArray")
				.setSplits(new String[]{"d", "e", "f"});
		graph = (AccumuloGraph) GraphFactory.open(cfg.getConfiguration());
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
	@Test
	public void testPropertyValues() throws Exception {
		AccumuloGraph graph = new AccumuloGraph(AccumuloGraphTestUtils.generateGraphConfig("propertyValues"));
		// Tests for serialization/deserialization of properties.
		QName qname = new QName("ns", "prop");
		Vertex v = graph.addVertex(null);
		v.setProperty("qname", qname);
		assertTrue(v.getProperty("qname") instanceof QName);
		assertTrue(qname.equals(v.getProperty("qname")));
	}
	@Test
	public void testCreateAndClear() throws Exception {
		AccumuloGraphConfiguration cfg =
				AccumuloGraphTestUtils.generateGraphConfig("noCreate").create(false);
		try {
			new AccumuloGraph(cfg);
			fail("Create is disabled and graph does not exist");
		} catch (Exception e) {
			assertTrue(true);
		}

		cfg = AccumuloGraphTestUtils.generateGraphConfig("yesCreate").create(true);
		for (String t : cfg.getTableNames()) {
			assertFalse(cfg.getConnector().tableOperations().exists(t));
		}
		AccumuloGraph graph = new AccumuloGraph(cfg);
		for (String t : cfg.getTableNames()) {
			assertTrue(cfg.getConnector().tableOperations().exists(t));
		}
		graph.shutdown();

		graph = new AccumuloGraph(cfg.create(false));
		assertTrue(graph.isEmpty());
		graph.addVertex("A");
		graph.addVertex("B");
		assertFalse(graph.isEmpty());
		graph.shutdown();

		graph = new AccumuloGraph(cfg.setClear(true));
		assertTrue(graph.isEmpty());
		graph.shutdown();
	}
	@Test
	public void testIsEmpty() throws Exception {
		AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils.generateGraphConfig("isEmpty");
		AccumuloGraph graph = new AccumuloGraph(cfg);
		assertTrue(graph.isEmpty());

		graph.addVertex("A");
		assertFalse(graph.isEmpty());

		graph.clear();
		assertTrue(graph.isEmpty());
	}
}
