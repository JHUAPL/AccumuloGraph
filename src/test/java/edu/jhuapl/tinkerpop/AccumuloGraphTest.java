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

import java.lang.reflect.Method;

import com.tinkerpop.blueprints.EdgeTestSuite;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.IndexTestSuite;
import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class AccumuloGraphTest extends GraphTest {
    
    // Breaking build on purpose to test jenkins server
    broken

	ThreadLocal<String> testGraphName = new ThreadLocal<String>();

	public void testVertexTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new VertexTestSuite(this));
		printTestPerformance("VertexTestSuite", this.stopWatch());
	}

	public void testEdgeTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new EdgeTestSuite(this));
		printTestPerformance("EdgeTestSuite", this.stopWatch());
	}

	public void testGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new GraphTestSuite(this));
		printTestPerformance("GraphTestSuite", this.stopWatch());
	}

	public void testAccumuloGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new AccumuloGraphTestSuite());
		printTestPerformance("AccumuloGraphTestSuite", this.stopWatch());
	}

	public void testKeyIndexableGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new KeyIndexableGraphTestSuite(this));
		printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
	}

	public void testIndexableGraphTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new IndexableGraphTestSuite(this));
		printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
	}

	public void testIndexTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new IndexTestSuite(this));
		printTestPerformance("IndexTestSuite", this.stopWatch());
	}

	public void testGraphMLReaderTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new GraphMLReaderTestSuite(this));
		printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
	}

	public void testGMLReaderTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new GMLReaderTestSuite(this));
		printTestPerformance("GMLReaderTestSuite", this.stopWatch());
	}

	public void testGraphSONReaderTestSuite() throws Exception {
		this.stopWatch();
		doTestSuite(new GraphSONReaderTestSuite(this));
		printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
	}

	public void doTestSuite(final TestSuite testSuite) throws Exception {
		String doTest = System.getProperty("testTinkerGraph");
		if (doTest == null || doTest.equals("true")) {
			for (Method method : testSuite.getClass().getDeclaredMethods()) {
				if (method.getName().startsWith("test")) {
					System.out.println("Testing " + method.getName() + "...");
					method.invoke(testSuite);
					dropGraph(testGraphName.get());
				}
			}
		}
	}

	@Override
	public Graph generateGraph() {
		return generateGraph("AAA_test_graph");
	}

	static {
		// So setting up the Accumulo Mock Cluster does not affect the test
		// times
		new AccumuloGraphTest().generateGraph();
	}

	public void dropGraph(final String graphDirectoryName) {
		if (graphDirectoryName != null) {
			((AccumuloGraph) generateGraph(graphDirectoryName)).clear();
		}
	}

	public Object convertId(final Object id) {
		return id.toString();
	}

	@Override
	public Graph generateGraph(String graphDirectoryName) {
		AccumuloGraphConfiguration cfg =
				AccumuloGraphTestUtils.generateGraphConfig(graphDirectoryName);
		testGraphName.set(graphDirectoryName);
		return GraphFactory.open(cfg);
	}
}