package edu.jhuapl.tinkerpop;

import org.apache.commons.configuration.Configuration;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

/**
 * Run the test suite for a Mock instance type.
 */
public class MockInstanceTest extends AccumuloGraphTest {

  @Override
  public Graph generateGraph(String graphDirectoryName) {
    Configuration cfg = new AccumuloGraphConfiguration()
      .setInstanceType(InstanceType.Mock)
      .setGraphName(graphDirectoryName);
    testGraphName.set(graphDirectoryName);
    return GraphFactory.open(cfg);
  }
}
