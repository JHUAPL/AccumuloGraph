/**
 * 
 */
package edu.jhuapl.tinkerpop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.TestSuite;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

/**
 * @author Michael Lieberman (http://mikelieberman.org)
 *
 */
public class AccumuloGraphTestSuite extends TestSuite {

	public void testSplits() throws Exception {
		AccumuloGraphConfiguration cfg;

		cfg = getGraphConfig("nullSplits").splits((String) null);
		Graph graph = GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = getGraphConfig("emptySplits").splits("");
		graph = GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			assertEquals(0, cfg.getConnector().tableOperations().listSplits(table).size());
		}
		graph.shutdown();

		cfg = getGraphConfig("threeSplits").splits(" a b c ");
		graph = GraphFactory.open(cfg);
		for (String table : cfg.getTableNames()) {
			Collection<Text> splits = cfg.getConnector().tableOperations().listSplits(table);
			assertEquals(3, splits.size());
			List<Text> arr = new ArrayList<Text>(splits);
			assertEquals("a", arr.get(0).toString());
			assertEquals("b", arr.get(1).toString());
			assertEquals("c", arr.get(2).toString());
		}
		graph.shutdown();
	}

	//public void testSplitsArray() {
	//	AccumuloGraphConfiguration cfg = getGraphConfig("testSplitsArray");		
	//}

	private AccumuloGraphConfiguration getGraphConfig(String graphDirectoryName) {
		AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration();
		cfg.instance("instanceName").zkHosts("ZookeeperHostsString");
		cfg.user("root").password("".getBytes());
		cfg.name(graphDirectoryName).create(true).autoFlush(true)
				.instanceType(InstanceType.Mock).lruMaxCapacity(10)
				.propertyCacheTimeout(10000);
		return cfg;
	}
}
