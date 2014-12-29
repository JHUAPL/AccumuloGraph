package edu.jhuapl.tinkerpop;

import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

/**
 * Run the test suite for a Distributed instance type.
 * <p/>Note: This is disabled by default since we can't
 * guarantee an Accumulo cluster setup in a test environment.
 * To use this, set the constants below and remove
 * the @Ignore annotation.
 */
@Ignore
public class DistributedInstanceTest extends AccumuloGraphTest {

  // Connection constants.
  private static final String ZOOKEEPERS = "zkhost-1,zkhost-2";
  private static final String INSTANCE = "accumulo-instance";
  private static final String USER = "user";
  private static final String PASSWORD = "password";

  @Override
  public Graph generateGraph(String graphDirectoryName) {
    Configuration cfg = new AccumuloGraphConfiguration()
    .setInstanceType(InstanceType.Distributed)
    .setZooKeeperHosts(ZOOKEEPERS)
    .setInstanceName(INSTANCE)
    .setUser(USER).setPassword(PASSWORD)
    .setGraphName(graphDirectoryName);
  testGraphName.set(graphDirectoryName);
  return GraphFactory.open(cfg);
  }
}
