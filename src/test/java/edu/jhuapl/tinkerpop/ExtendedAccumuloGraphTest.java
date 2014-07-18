package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;

public class ExtendedAccumuloGraphTest extends AccumuloGraphTest {
	@Override
	public Graph generateGraph(String graphDirectoryName) {
		AccumuloGraphConfiguration cfg =
				AccumuloGraphTestUtils.generateGraphConfig(graphDirectoryName);
		cfg.setLruMaxCapacity(20).setPreloadedProperties(new String[]{"test"})
			.setPreloadedEdgeLabels(new String[]{"knows"})
			.setPropertyCacheTimeout(100000);
		testGraphName.set(graphDirectoryName);
		return GraphFactory.open(cfg.getConfiguration());
	}
}
