/**
 * 
 */
package edu.jhuapl.tinkerpop;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

/**
 * @author Michael Lieberman (http://mikelieberman.org)
 *
 */
public class AccumuloGraphTestUtils {

	public static AccumuloGraphConfiguration generateGraphConfig(String graphDirectoryName) {
		AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration();
		cfg.instance("instanceName").zkHosts("ZookeeperHostsString");
		cfg.user("root").password("".getBytes());
		cfg.name(graphDirectoryName).create(true).autoFlush(true)
				.instanceType(InstanceType.Mock).lruMaxCapacity(10)
				.propertyCacheTimeout(10000);
		return cfg;
	}

}
