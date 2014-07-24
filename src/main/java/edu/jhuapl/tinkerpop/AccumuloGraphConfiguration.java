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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Text;

public class AccumuloGraphConfiguration  implements	Serializable {

	private Configuration conf; 

	/**
	 * The fully-qualified class name of the class that implements the TinkerPop
	 * Graph interface. This is used in a configuration object to tell the
	 * GraphFactory which type to instantiate.
	 */
	public static final String ACCUMULO_GRAPH_CLASSNAME = AccumuloGraph.class
			.getCanonicalName();

	private static final long serialVersionUID = 7024072260167873696L;

	/**
	 * An enumeration used by
	 * {@link AccumuloGraphConfiguration#instanceType(InstanceType)} to specify
	 * the backing Accumulo instance type. See the <A HREF=
	 * "http://accumulo.apache.org/1.6/accumulo_user_manual.html#_development_clients"
	 * >Accumulo Users' Guide</A> for more information on the differnent types
	 * of develoment clients.
	 * 
	 */
	public static enum InstanceType {
		Distributed, Mini, Mock
	};

	public static final String GRAPH_CLASS = "blueprints.graph";
	public static final String ZK_HOSTS = "blueprints.accumulo.zkhosts";
	public static final String INSTANCE = "blueprints.accumulo.instance";
	public static final String INSTANCE_TYPE = "blueprints.accumulo.instance.type";
	public static final String USER = "blueprints.accumulo.user";
	public static final String PASSWORD = "blueprints.accumulo.password";
	public static final String GRAPH_NAME = "blueprints.accumulo.name";
	public static final String MAX_WRITE_LATENCY = "blueprints.accumulo.write.max.latency";
	public static final String MAX_WRITE_MEMORY = "blueprints.accumulo.write.max.memory";
	public static final String MAX_WRITE_THREADS = "blueprints.accumulo.write.max.threads";
	public static final String MAX_WRITE_TIMEOUT = "blueprints.accumulo.write.timeout";
	public static final String QUERY_THREADS = "blueprints.accumulo.read.queryThreads";
	public static final String AUTHORIZATIONS = "blueprints.accumulo.authorizations";
	public static final String AUTO_FLUSH = "blueprints.accumulo.auto.flush";
	public static final String CREATE = "blueprints.accumulo.create";
	public static final String CLEAR = "blueprints.accumulo.clear";
	public static final String SPLITS = "blueprints.accumulo.splits";
	public static final String COLVIS = "blueprints.accumulo.columnVisibility";
	public static final String SKIP_CHECKS = "blueprints.accumulo.skipExistenceChecks";
	public static final String LRU_MAX_CAP = "blueprints.accumulo.lruMaximumCapacity";
	public static final String PRELOAD_PROPERTIES = "blueprints.accumulo.property.preload";
	public static final String EDGE_CACHE_TIMEOUT = "blueprints.accumulo.edgeCacheTimeout";
	public static final String PROPERTY_CACHE_TIMEOUT = "blueprints.accumulo.propertyCacheTimeout";
	public static final String VERTEX_CACHE_TIMEOUT = "blueprints.accumulo.vertexCacheTimeout";
	public static final String PRELOAD_EDGES = "blueprints.accumulo.edge.preload";

	/**
	 * Backing store that maintains configuration values.
	 */
	private Map<String, Object> values;

	private transient ColumnVisibility cachedColVis = null;
	private transient Authorizations cachedAuths = null;
	private transient Boolean cachedAutoFlush = null;
	private transient Boolean cachedSkipChecks = null;
	/**
	 * Temp directory used by getInstance when a Mini InstanceType is used.
	 */
	private String miniClusterTempDir;
	private MiniAccumuloCluster accumuloMiniCluster;

	public AccumuloGraphConfiguration() {
		conf = new PropertiesConfiguration();
		values = new HashMap<String, Object>();

		values.put(GRAPH_CLASS, ACCUMULO_GRAPH_CLASSNAME);
		conf.addProperty(GRAPH_CLASS, ACCUMULO_GRAPH_CLASSNAME);
		// set some defaults
		setMaxWriteLatency(60000L).setMaxWriteMemory(1024L * 1024 * 20)
				.setMaxWriteThreads(3).setMaxWriteTimeout(Long.MAX_VALUE)
				.autoFlush(true).create(false)
				.setInstanceType(InstanceType.Distributed)
				.setAuthorizations(Constants.NO_AUTHS).setQueryThreads(3)
				.skipExistenceChecks(false);
	}

	public AccumuloGraphConfiguration(Configuration config) {
		conf = new PropertiesConfiguration();

		Iterator<String> keys = config.getKeys();
		while (keys.hasNext()) {
			String key = keys.next();
			conf.addProperty(key.replace("..", "."), config.getProperty(key));
		}
	}

	/**
	 * If the graph does not exist, whether it should be created.
	 * An exception will be throws on instantiation if the graph
	 * does not exist and this value is false.
	 * @param create
	 * @return
	 */
	public AccumuloGraphConfiguration create(boolean create) {
		conf.setProperty(CREATE, create);
		return this;
	}

	/**
	 * Whether to clear out an existing graph upon instantiation.
	 * @param clear
	 * @return
	 */
	public AccumuloGraphConfiguration setClear(boolean clear) {
		conf.setProperty(CLEAR, clear);
		return this;
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public AccumuloGraphConfiguration setZookeeperHosts(String zookeeperHosts) {
		conf.setProperty(ZK_HOSTS, zookeeperHosts);
		return this;
	}

	public AccumuloGraphConfiguration setInstanceName(String instance) {
		conf.setProperty(INSTANCE, instance);
		return this;
	}

	public AccumuloGraphConfiguration setUser(String user) {
		conf.setProperty(USER, user);
		return this;
	}

	/**
	 * The TinkerPop API defines certain operations should fail if a Vertex or
	 * Edge already exists or does not exist. For instance a call to getVertex()
	 * must first check if the vertex exists and, if not, return null. Likewise
	 * a call to addVertex() must first check if the vertex already exists and
	 * if so, throw and exception.
	 * <P>
	 * However since the AccumuloGraph does not assume (or attempt to maintain)
	 * the entire graph in RAM, these checks require a complete round-trip to
	 * the Accumulo instance to determine existence.
	 * <P>
	 * In some instances, the user may decide the existence checks are not
	 * needed and would rather not incur the round-trip costs. By setting this
	 * flag to true, the AccumuloGraph will not perform existence checks. A
	 * request to getVertex() will always return a Vertex instance. It is only
	 * when requesting data from that instance the AccumuloGraph will reach out
	 * to the Accumulo instance to determine if the Vertex actually exists.
	 * <P>
	 * In this case it is up to the user's code to ensure that requests for
	 * vertices are valid, requests to create new vertices are unique, etc.
	 * <P>
	 * Note that multiple requests to create the same node does not actually
	 * break the AccumuloGraph. The "existence" key/value pair identifying that
	 * ID as a Vertex in the vertex table will simply be repeated. The repeated
	 * key/value pair will eventually be collapsed back to a single pair (on the
	 * next compaction of the Accumulo graph). The trade-off here is additional
	 * I/O sending (potentially) duplicate create messages when nodes are
	 * repeated versus requiring a round-trip on every node create. In cases
	 * where repeated nodes are infrequent, it may be more efficient to simply
	 * (re-)create the Vertex rather than find the node in the graph.
	 * <P>
	 * That is, with skipExistenceChecks set to false:
	 * 
	 * <pre>
	 * Vertex src = graph.addVertex(srcID);
	 * Vertex dest = graph.addVertex(destID);
	 * graph.addEdge(null, src, dest, &quot;myEdge&quot;);
	 * </pre>
	 * 
	 * may be faster than:
	 * 
	 * <pre>
	 * Vertex src = graph.getVertex(srcID);
	 * if (src == null) {
	 * 	src = graph.addVertex(srcId);
	 * }
	 * Vertex dest = graph.getVertex(destID);
	 * if (dest == null) {
	 * 	dest = graph.addVertex(destID);
	 * }
	 * graph.addEdge(null, src, dest, &quot;myEdge&quot;);
	 * </pre>
	 * 
	 * if you are creating many edges where source and destination edges are
	 * infrequently repeated.
	 * <P>
	 * Similarly, in instances where the application can guarantee that source
	 * and destinations vertices for a given new edge have already been added to
	 * the Graph, skipping the existence checks for the source and destination
	 * vertices and utilizing getVertex() directly can speed the processing.
	 * <P>
	 * This flag defaults to false (checks will be made).
	 * 
	 * @param skip
	 * @return
	 */
	public AccumuloGraphConfiguration skipExistenceChecks(boolean skip) {
		conf.setProperty(SKIP_CHECKS, skip);
		return this;
	}

	/**
	 * Sets the number of milliseconds since retrieval that a property value
	 * will be maintained in a RAM cache before that value is expired. If this
	 * value is unset or set to 0 (or a negative number) no caching will be
	 * performed.
	 * <P>
	 * A round-trip to Accumulo to retrieve a property value is an expensive
	 * operation. Setting this value to a positive number allows the
	 * AccumuloGraph to cache retrieved values and use those values (without
	 * re-consulting the backing Accumulo store) for the specified time. In
	 * situations where the graph is changing slowly and/or properties are
	 * revisited frequently, this can achieve a significant reduction in latency
	 * at the expense of consistency.
	 * <P>
	 * The default is unset (no caching).
	 * 
	 * @param millis
	 *            the maximum number of milliseconds properties can be held in
	 *            RAM
	 * @return
	 */
	public AccumuloGraphConfiguration setPropertyCacheTimeout(int millis) {
		if (millis <= 0) {
			conf.clearProperty(PROPERTY_CACHE_TIMEOUT);
		} else {
			conf.setProperty(PROPERTY_CACHE_TIMEOUT, millis);
		}
		return this;
	}

	/**
	 * Sets the number of milliseconds since retrieval that a Vertex instance
	 * will be maintained in a RAM cache before that value is expired. If this
	 * value is unset or set to 0 (or a negative number) no caching will be
	 * performed.
	 * <P>
	 * A round-trip to Accumulo to retrieve a Vertex is an expensive operation.
	 * Setting this value to a positive number allows the AccumuloGraph to cache
	 * retrieved Vertices and use those references (without re-consulting the
	 * backing Accumulo store) for the specified time. In situations where the
	 * graph is changing slowly and/or Vertices are revisited frequently, this
	 * can achieve a significant reduction in latency at the expense of
	 * consistency.
	 * <P>
	 * The default is unset (no caching).
	 * 
	 * @param millis
	 *            the maximum number of milliseconds a Vertex should be held in
	 *            RAM
	 * @return
	 */
	public AccumuloGraphConfiguration setVertexCacheTimeout(int millis) {
		if (millis <= 0) {
			conf.clearProperty(VERTEX_CACHE_TIMEOUT);
		} else {
			conf.setProperty(VERTEX_CACHE_TIMEOUT, millis);
		}
		return this;
	}

	/**
	 * Sets the number of milliseconds since retrieval that an Edge instance
	 * will be maintained in a RAM cache before that value is expired. If this
	 * value is unset or set to 0 (or a negative number) no caching will be
	 * performed.
	 * <P>
	 * A round-trip to Accumulo to retrieve an Edge is an expensive operation.
	 * Setting this value to a positive number allows the AccumuloGraph to cache
	 * retrieved Edges and use those references (without re-consulting the
	 * backing Accumulo store) for the specified time. In situations where the
	 * graph is changing slowly and/or Edges are revisited frequently, this can
	 * achieve a significant reduction in latency at the expense of consistency.
	 * <P>
	 * The default is unset (no caching).
	 * 
	 * @param millis
	 *            the maximum number of milliseconds an Edge should be held in
	 *            RAM
	 * @return
	 */
	public AccumuloGraphConfiguration setEdgeCacheTimeout(int millis) {
		if (millis <= 0) {
			conf.clearProperty(EDGE_CACHE_TIMEOUT);
		} else {
			conf.setProperty(EDGE_CACHE_TIMEOUT, millis);
		}
		return this;
	}

	/**
	 * The number of query threads to use when accessing the backing Accumulo
	 * store. This value must be greater than or equal to one or an
	 * IllegalArgumentException will be thrown.
	 * 
	 * @param threads
	 * @return
	 */
	public AccumuloGraphConfiguration setQueryThreads(int threads) {
		if (threads < 1) {
			throw new IllegalArgumentException(
					"You must provide at least 1 query thread.");
		}
		conf.setProperty(QUERY_THREADS, threads);
		return this;
	}

	public AccumuloGraphConfiguration setInstanceType(InstanceType type) {
		conf.setProperty(INSTANCE_TYPE, type.toString());
		return this;
	}

	public AccumuloGraphConfiguration setColumnVisibility(ColumnVisibility colVis) {
		conf.setProperty(COLVIS, new String(colVis.flatten()));
		return this;
	}

	/**
	 * The Graph can utilize an least-recently used (LRU) cache to avoid
	 * round-trip checks to Accumulo at the cost of consistency. Set this value
	 * to the maximum number of vertices or edges to be cached. A negative
	 * number means do not cache any values.
	 * 
	 * TODO this probably should be a time-based cache eventually.
	 * 
	 * @param maxSize
	 * @return
	 */
	public AccumuloGraphConfiguration setLruMaxCapacity(int max) {
		conf.setProperty(LRU_MAX_CAP, max);
		return this;
	}

	/**
	 * A space-separated, ordered list of splits to be applied to the backing
	 * Accumulo-table. Only applied if the graph does not already exist and the
	 * config {@link #create(boolean)} is set to true.
	 * 
	 * @param splits
	 * @return
	 */
	public AccumuloGraphConfiguration setSplits(String splits) {
		if (splits == null || splits.trim().isEmpty()) {
			return this;
		}
		return setSplits(splits.trim().split(" "));
	}

	public AccumuloGraphConfiguration setSplits(String[] splits) {
		conf.setProperty(SPLITS, splits != null ? Arrays.asList(splits) : null);
		return this;
	}

	public AccumuloGraphConfiguration setPassword(byte[] password) {
		conf.setProperty(PASSWORD, new String(password));
		return this;
	}
	public AccumuloGraphConfiguration setPassword(String password) {
		conf.setProperty(PASSWORD, password);
		return this;
	}
	
	/**
	 * Used by JUnit Tests to set the miniClusterTempDirectory.  If not set in advance
	 * of a test, getConnector will use a Java Temporary Folder which will not be 
	 * deleted afterwards.
	 * @param miniClusterTempDir
	 */
	public void setMiniClusterTempDir(String miniClusterTempDir) {
	    this.miniClusterTempDir = miniClusterTempDir;
	}
	

	/**
	 * A flag if the AccumuloGraph should immediately flush each update to the
	 * backing AccumuloStore (true) or not (false). The TinkerPop API expects
	 * immediate consistency requiring each individual update to be immediately
	 * flushed to Accumulo. However, this incurs significant overhead. For
	 * applications at scale that do not require immediate consistency, this
	 * flag allows the user to lessen the TinkerPop restriction.
	 * <P>
	 * To support the expected TinkerPop behavior, the default value is true.
	 * However, it is strongly recommended that this option be disabled.
	 * 
	 * @param autoFlush
	 * @return
	 */
	public AccumuloGraphConfiguration autoFlush(boolean autoFlush) {
		conf.setProperty(AUTO_FLUSH, autoFlush);
		return this;
	}

	public AccumuloGraphConfiguration setGraphName(String name) {
		conf.setProperty(GRAPH_NAME, name);
		return this;
	}

	public AccumuloGraphConfiguration setMaxWriteLatency(long latency) {
		if (latency < 0) {
			throw new IllegalArgumentException(
					"Maximum write latency must be a postive number, "
							+ "or '0' for no maximum.");
		}

		conf.setProperty(MAX_WRITE_LATENCY, latency);
		return this;
	}

	public AccumuloGraphConfiguration setMaxWriteTimeout(long timeout) {
		if (timeout < 0) {
			throw new IllegalArgumentException(
					"Maximum write timeout must be a postive number, "
							+ "or '0' for no maximum.");
		}

		conf.setProperty(MAX_WRITE_TIMEOUT, timeout);
		return this;
	}

	/**
	 * A trip to the backing-Accumulo store to obtain data is a relatively
	 * expensive operation. In cases where the end-user knows there are certain
	 * sets of properties that will always/very likely be obtained, it may be
	 * more efficient to grab all of those properties at once as the element
	 * existence is confirmed. In other cases (e.g., rarely used or very large
	 * properties) it may be more efficient to wait to obtain the data until the
	 * program determines the property is in fact needed.
	 * <P>
	 * Deferred property loading is the default. By setting this configuration
	 * value, any keys in the provided property list will be automatically
	 * loaded in bulk when it makes sense (i.e., when the system has to make a
	 * trip out to Accumulo anyway). Other proerties not in the list will
	 * continue to be lazily and individually loaded.
	 * <P>
	 * In order to set this value, you must first define a postive property
	 * cache timeout value ({@link #propertyCacheTimeout(int)}; it does not make
	 * sense to pre-load data if you do not allow caching.
	 * 
	 * @param propertyKeys
	 * @return
	 */
	public AccumuloGraphConfiguration setPreloadedProperties(String[] propertyKeys) {
		if (propertyKeys == null) {
			throw new NullPointerException("Property keys cannot be null.");
		}


		conf.setProperty(PRELOAD_PROPERTIES, propertyKeys);
		return this;
	}

	public AccumuloGraphConfiguration setPreloadedEdgeLabels(String[] edgeLabels) {
		if (edgeLabels == null) {
			throw new NullPointerException("Edge labels cannot be null.");
		}

		Integer timeout = getEdgeCacheTimeoutMillis();
		if (timeout == null) {
			throw new IllegalArgumentException("You cannot preload edges "
					+ "without first setting #edgeCacheTimeout(int millis) "
					+ "to a positive value.");
		}

		conf.setProperty(PRELOAD_EDGES, edgeLabels);
		return this;
	}

	public AccumuloGraphConfiguration setMaxWriteMemory(long mem) {
		if (mem <= 0) {
			throw new IllegalArgumentException(
					"Maximum write memory must be a postive number.");
		}
		conf.setProperty(MAX_WRITE_MEMORY, mem);
		return this;
	}

	public AccumuloGraphConfiguration setMaxWriteThreads(int threads) {
		if (threads < 1) {
			throw new IllegalArgumentException(
					"Maximum write threads must be a postive number.");
		}
		conf.setProperty(MAX_WRITE_THREADS, threads);
		return this;
	}

	public AccumuloGraphConfiguration setAuthorizations(Authorizations auths) {
		byte[] data = auths.getAuthorizationsArray();
		conf.setProperty(AUTHORIZATIONS, new String(data));
		return this;
	}

	public boolean isCreate() {
		return conf.getBoolean(CREATE);
	}

	public boolean isClear() {
		return conf.getBoolean(CLEAR, false);
	}

	public InstanceType getInstanceType() {
		return InstanceType.valueOf(conf.getString(INSTANCE_TYPE));
	}

	public Authorizations getAuthorizations() {
		if (cachedAuths == null) {
			String auths = conf.getString(AUTHORIZATIONS);
			if (auths != null) {
				cachedAuths = new Authorizations(auths.getBytes());
			}
		}
		return cachedAuths;
	}

	public String getUser() {
		return conf.getString(USER);
	}

	public ByteBuffer getPassword() {
		return ByteBuffer.wrap(conf.getString(PASSWORD).getBytes());
	}

	public String getInstance() {
		return conf.getString(INSTANCE);
	}

	public String getZooKeeperHosts() {
		return conf.getString(ZK_HOSTS);
	}

	public boolean isAutoFlush() {
		if (cachedAutoFlush == null) {
			cachedAutoFlush = conf.getBoolean(AUTO_FLUSH);
		}
		return cachedAutoFlush;
	}

	public Integer getPropertyCacheTimeoutMillis() {
		return conf.getInteger(PROPERTY_CACHE_TIMEOUT, -1);
	}

	public Integer getEdgeCacheTimeoutMillis() {
		return conf.getInteger(EDGE_CACHE_TIMEOUT, 30000);
	}

	public Integer getVertexCacheTimeoutMillis() {
		return conf.getInteger(VERTEX_CACHE_TIMEOUT, 30000);
	}

	public boolean skipExistenceChecks() {
		if (cachedSkipChecks == null) {
			cachedSkipChecks = conf.getBoolean(SKIP_CHECKS);
		}
		return cachedSkipChecks;
	}

	public long getMaxWriteMemory() {
		return conf.getLong(MAX_WRITE_MEMORY);
	}

	public long getMaxWriteTimeout() {
		return conf.getLong(MAX_WRITE_TIMEOUT);
	}

	public BatchWriterConfig getBatchWriterConfig() {
		return new BatchWriterConfig()
				.setMaxLatency(this.getMaxWriteLatency(), TimeUnit.MILLISECONDS)
				.setMaxMemory(this.getMaxWriteMemory())
				.setMaxWriteThreads(this.getMaxWriteThreads())
				.setTimeout(getMaxWriteTimeout(), TimeUnit.MILLISECONDS);
	}

	public SortedSet<Text> getSplits() {
		String[] val = conf.getStringArray(SPLITS);
		if ((val == null) || (val.length == 0)) {
			return null;
		}
		SortedSet<Text> splits = new TreeSet<Text>();
		for (String s : val) {
			splits.add(new Text(s));
		}
		return splits;
	}

	public long getMaxWriteLatency() {
		return conf.getLong(MAX_WRITE_LATENCY);
	}

	public int getMaxWriteThreads() {
		return conf.getInt(MAX_WRITE_THREADS);
	}

	public String getName() {
		return conf.getString(GRAPH_NAME);
	}

	public boolean useLruCache() {
		return getLruMaxCapacity() > 0;
	}

	public int getLruMaxCapacity() {
		return conf.getInt(LRU_MAX_CAP, -1);
	}

	public ColumnVisibility getColumnVisibility() {
		if (cachedColVis == null) {
			cachedColVis = new ColumnVisibility(conf.getString(COLVIS).getBytes());
		}
		return cachedColVis;
	}

	public Connector getConnector() throws AccumuloException,
			AccumuloSecurityException, IOException, InterruptedException {
		Instance inst = null;
		switch (getInstanceType()) {
		case Distributed:
			inst = new ZooKeeperInstance(getInstance(), getZooKeeperHosts());
			break;
		case Mini:
		    File dir = null;
		    if(miniClusterTempDir == null) {
	            dir = createTempDir();
	            dir.deleteOnExit();
		    } else {
		        // already set by setMiniClusterTempDir(), It should be cleaned up outside of this class.
		        dir = new File(miniClusterTempDir);
		    }
		    accumuloMiniCluster = new MiniAccumuloCluster(dir, "" );  //conf.getString(PASSWORD)
		    try {
		        accumuloMiniCluster.start();
		    } catch (Exception ex ) {
		        ex.printStackTrace();
		        System.out.println("");
		    }
		    inst = new ZooKeeperInstance(accumuloMiniCluster.getInstanceName(), accumuloMiniCluster.getZooKeepers());
			throw new UnsupportedOperationException("TODO");
		case Mock:
			inst = new MockInstance(getInstance());
			break;
		default:
			throw new RuntimeException("Unexpected instance type: " + inst);
		}
		
		Connector c = inst.getConnector(getUser(), new PasswordToken(
				getPassword()));
		return c;
	}

	public String[] getPreloadedProperties() {
		if (containsKey(PRELOAD_PROPERTIES)) {
			return conf.getStringArray(PRELOAD_PROPERTIES);
		}
		return null;
	}

	public String[] getPreloadedEdges() {
		if (containsKey(PRELOAD_EDGES)) {
			return conf.getStringArray(PRELOAD_EDGES);
		}
		return null;
	}

	public int getQueryThreads() {
		return conf.getInt(QUERY_THREADS);
	}

	public boolean containsKey(String key) {
		return conf.containsKey(key);
	}

	public boolean isEmpty() {
		return conf.isEmpty();
	}

	public String getVertexTable() {
		return getName() + "_vertex";
	}

	public String getEdgeTable() {
		return getName() + "_edge";
	}

	String getVertexIndexTable() {
		return getName() + "_vertex_index";
	}

	String getEdgeIndexTable() {
		return getName() + "_edge_index";
	}

	String getMetadataTable() {
		return getName() + "_meta";
	}

	List<String> tableList;

	List<String> getTableNames() {
		if (tableList == null) {
			tableList = Arrays.asList(getVertexTable(), getEdgeTable(),
					getVertexIndexTable(), getEdgeIndexTable(),
					getMetadataTable(), getKeyMetadataTable());
		}
		return tableList;
	}

	
	protected void addPropertyDirect(String key, Object value) {
		if ((key.equals(PRELOAD_PROPERTIES)) || (key.equals(PRELOAD_EDGES)) || (key.equals(SPLITS))) {
			List<String> list = (List<String>) values.get(key);
			if (list == null) {
				list = new ArrayList<String>();
				values.put(key, list);
			}
			list.add(value.toString());
		} else {
			values.put(key, value);
		}
	}

	public void validate() {

		switch (getInstanceType()) {
		case Distributed:
			checkPropertyValue(ZK_HOSTS, getZooKeeperHosts(), false);
			checkPropertyValue(USER, getUser(), false);
			// no break intentional
		case Mini:
			checkPropertyValue(INSTANCE, getInstance(), false);
			checkPropertyValue(PASSWORD, new String(getPassword().array()),
					true);
			// no break intentional
		case Mock:
			checkPropertyValue(GRAPH_NAME, getName(), false);
			break;
		default:
			throw new RuntimeException("Unexpected instance type: "
					+ getInstanceType());
		}
		

		Integer timeout = getPropertyCacheTimeoutMillis();
		
		if (timeout < 0 && conf.getProperty(PRELOAD_PROPERTIES) != null) {
			throw new IllegalArgumentException(
					"You cannot preload properties "
							+ "without first setting #propertyCacheTimeout(int millis) "
							+ "to a positive value.");
		}
	}

	private void checkPropertyValue(String prop, String val, boolean canBeEmpty) {
		if (val == null) {
			throw new AccumuloGraphException(prop + " cannot be null.");
		}
		if ((!canBeEmpty) && (val.equals(""))) {
			throw new AccumuloGraphException(prop + " cannot be empty.");
		}
	}

	public String getKeyMetadataTable() {
		return getMetadataTable() + "KEY";
	}

	String getKeyVertexIndexTable() {
		return getName() + "_vertex_index_key";
	}

	String getKeyEdgeIndexTable() {
		return getName() + "_edge_index_key";
	}
	
	/**
	 * Creates a temporary directory.  Under the  
	 * java.io.tmpdir property location.
	 * @return
	 */
    private File createTempDir() {
        final int ATTEMPTS=100;
        File parent = new File(System.getProperty("java.io.tmpdir"));
        String child = System.currentTimeMillis() + "-";

        for (int counter = 0; counter < ATTEMPTS; counter++) {
          File tempDir = new File(parent, child + counter);
          if (tempDir.mkdir()) {
            return tempDir;
          }
        }
        throw new IllegalStateException("Failed to create directory, tried directories: "
          + child + "0 to " + child + (ATTEMPTS - 1) );
      }





}
