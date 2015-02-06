package edu.jhuapl.tinkerpop;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;

public class ExtendedAccumuloGraphTest extends AccumuloGraphTest {

  @Override
  public Graph generateGraph(String graphDirectoryName) {
    AccumuloGraphConfiguration cfg = AccumuloGraphTestUtils.generateGraphConfig(graphDirectoryName);
    cfg.setEdgeCacheParams(20, 30000)
    .setPreloadedProperties(new String[] {"name"})
    .setPreloadedEdgeLabels(new String[] {"knows"})
    .setPropertyCacheTimeout("name", 100000);
    testGraphName.set(graphDirectoryName);
    return GraphFactory.open(cfg.getConfiguration());
  }

}
