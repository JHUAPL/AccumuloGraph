/**
 * 
 */
package edu.jhuapl.tinkerpop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.TestSuite;

/**
 * @author Michael Lieberman (http://mikelieberman.org)
 *
 */
public class AccumuloGraphTestSuite extends TestSuite {

	public void testSplits() throws Exception {
		AccumuloGraphConfiguration cfg;

		cfg = AccumuloGraphTestUtils.generateGraphConfig("nullSplits").splits((String) null);
		AccumuloGraph graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}

		cfg = AccumuloGraphTestUtils.generateGraphConfig("emptySplits").splits("");
		graph = (AccumuloGraph) GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}

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
	}

}