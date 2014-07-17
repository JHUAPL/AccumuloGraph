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

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationContext;
import com.tinkerpop.rexster.config.GraphConfigurationException;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class AccumuloRexsterGraphConfiguration implements GraphConfiguration {

	@Override
	public Graph configureGraphInstance(GraphConfigurationContext context)
			throws GraphConfigurationException {

		Configuration conf = context.getProperties();
		try {
			conf = ((HierarchicalConfiguration) conf)
					.configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
		} catch (IllegalArgumentException iae) {
			throw new GraphConfigurationException(
					"Check graph configuration. Missing or empty configuration element: "
							+ Tokens.REXSTER_GRAPH_PROPERTIES);
		}

		AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration(conf);
		if(cfg.getInstanceType().equals(InstanceType.Mock)){
			cfg.setPassword("".getBytes());
			cfg.setUser("root");
		}
		cfg.setGraphName(context.getProperties().getString(Tokens.REXSTER_GRAPH_NAME));
		return GraphFactory.open(cfg.getConfiguration());
	}

}
