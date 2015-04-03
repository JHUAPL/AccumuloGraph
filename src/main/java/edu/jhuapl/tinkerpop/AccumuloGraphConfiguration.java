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
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;

/**
 * Configuration class for setting AccumuloGraph parameters.
 * See the setters for descriptions of each attribute.
 * Setters return the same configuration instance to
 * ease chained setting of parameters.
 */
public class AccumuloGraphConfiguration extends AbstractConfiguration
implements Serializable {

  private static final long serialVersionUID = 7024072260167873696L;

  /**
   * Backing configuration object.
   */
  private PropertiesConfiguration conf;

  /**
   * Maintain a connector instance that will be
   * reused for {@link #getConnector()}.
   */
  private Connector connector;

  /**
   * Temp directory used by getInstance when a Mini InstanceType is used.
   */
  private String miniClusterTempDir;
  private MiniAccumuloCluster accumuloMiniCluster;


  /**
   * The {@link AccumuloGraph} class.
   */
  public static final Class<? extends Graph> ACCUMULO_GRAPH_CLASS = AccumuloGraph.class;

  /**
   * The fully-qualified class name of the class that implements
   * the TinkerPop Graph interface.
   * This is used in a configuration object to tell the GraphFactory
   * which type to instantiate.
   */
  public static final String ACCUMULO_GRAPH_CLASSNAME =
      ACCUMULO_GRAPH_CLASS.getCanonicalName();

  /**
   * An enumeration used by {@link AccumuloGraphConfiguration#setInstanceType(InstanceType)}
   * to specify the backing Accumulo instance type.
   * See the <A HREF="http://accumulo.apache.org/1.6/accumulo_user_manual.html#_development_clients">Accumulo Users' Guide</A>
   * for more information on the different types of development clients.
   * 
   */
  public static enum InstanceType {
    Distributed, Mini, Mock
  };

  /**
   * Utility class gathering valid configuration keys.
   */
  static class Keys {
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
    public static final String PRELOADED_PROPERTIES = "blueprints.accumulo.property.preload";
    public static final String PRELOAD_ALL_PROPERTIES = "blueprints.accumulo.property.preload.all";
    public static final String PROPERTY_CACHE_TIMEOUT = "blueprints.accumulo.propertyCacheTimeout";
    public static final String EDGE_CACHE_SIZE = "blueprints.accumulo.edgeCacheSize";
    public static final String EDGE_CACHE_TIMEOUT = "blueprints.accumulo.edgeCacheTimeout";
    public static final String VERTEX_CACHE_TIMEOUT = "blueprints.accumulo.vertexCacheTimeout";
    public static final String VERTEX_CACHE_SIZE = "blueprints.accumulo.vertexCacheSize";
    public static final String PRELOAD_EDGES = "blueprints.accumulo.edge.preload";
    public static final String AUTO_INDEX = "blueprints.accumulo.index.auto";
    public static final String DISABLE_INDEX = "blueprints.accumulo.index.disable";
  }


  /**
   * Default constructor. Will initialize the configuration
   * with default settings.
   */
  public AccumuloGraphConfiguration() {
    conf = new PropertiesConfiguration();
    conf.setProperty(Keys.GRAPH_CLASS, ACCUMULO_GRAPH_CLASSNAME);
    setDefaults();
  }

  /**
   * Instantiate based on a configuration 
   * @param config
   */
  public AccumuloGraphConfiguration(Configuration config) {
    this();
    Iterator<String> keys = config.getKeys();
    while (keys.hasNext()) {
      String key = keys.next();
      conf.setProperty(key.replace("..", "."),
          config.getProperty(key));
    }
  }

  /**
   * Copy constructor.
   * @param config
   */
  public AccumuloGraphConfiguration(AccumuloGraphConfiguration config) {
    this(config.getConfiguration());
  }

  /**
   * Set some reasonable defaults for this configuration.
   */
  private void setDefaults() {
    setMaxWriteLatency(60000L);
    setMaxWriteMemory(20L * 1024 * 1024);
    setMaxWriteThreads(3);
    setMaxWriteTimeout(Long.MAX_VALUE);
    setQueryThreads(3);
    setAutoFlush(true);
    setCreate(false);
    setInstanceType(InstanceType.Distributed);
    setAuthorizations(Constants.NO_AUTHS);
    setSkipExistenceChecks(false);
    setPreloadAllProperties(false);
  }

  /**
   * Return the backing configuration storage.
   * @return
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Create a copy of this configuration.
   * This is needed to change config parameters
   * if {@link #getConnector()} has been used.
   */
  public AccumuloGraphConfiguration clone() {
    return new AccumuloGraphConfiguration(this);
  }

  public boolean getCreate() {
    return conf.getBoolean(Keys.CREATE);
  }

  /**
   * If the graph does not exist, whether it should be created.
   * An exception will be thrown on instantiation if the graph
   * does not exist and this value is false.
   * 
   * @param create
   * @return
   */
  public AccumuloGraphConfiguration setCreate(boolean create) {
    conf.setProperty(Keys.CREATE, create);
    return this;
  }

  public boolean getClear() {
    return conf.getBoolean(Keys.CLEAR, false);
  }

  /**
   * Whether to clear an existing graph on initialization.
   * Clearing is done by dropping and recreating the backing tables.
   * @param clear
   * @return
   */
  public AccumuloGraphConfiguration setClear(boolean clear) {
    conf.setProperty(Keys.CLEAR, clear);
    return this;
  }

  public InstanceType getInstanceType() {
    return InstanceType.valueOf(conf.getString(Keys.INSTANCE_TYPE));
  }

  /**
   * Instance type to use with Accumulo (see {@link InstanceType}).
   * @param type
   * @return
   */
  public AccumuloGraphConfiguration setInstanceType(InstanceType type) {
    conf.setProperty(Keys.INSTANCE_TYPE, type.toString());

    if (InstanceType.Mock.equals(type) ||
        InstanceType.Mini.equals(type)) {
      setUser("root");
      setPassword("");
      setCreate(true);
    }
    if(InstanceType.Mock.equals(type)){
      setInstanceName("mock-instance");
    }

    return this;
  }

  public String getZooKeeperHosts() {
    return conf.getString(Keys.ZK_HOSTS);
  }

  /**
   * ZooKeeper hosts string, in the same format as used by
   * {@link ZooKeeperInstance}.
   * @param zookeeperHosts
   * @return
   */
  public AccumuloGraphConfiguration setZooKeeperHosts(String zookeeperHosts) {
    conf.setProperty(Keys.ZK_HOSTS, zookeeperHosts);
    return this;
  }

  public String getInstanceName() {
    return conf.getString(Keys.INSTANCE);
  }

  /**
   * Set Accumulo instance name to use.
   * @param instance
   * @return
   */
  public AccumuloGraphConfiguration setInstanceName(String instance) {
    conf.setProperty(Keys.INSTANCE, instance);
    return this;
  }

  public String getUser() {
    return conf.getString(Keys.USER);
  }

  /**
   * Username to use for Accumulo authentication.
   * @param user
   * @return
   */
  public AccumuloGraphConfiguration setUser(String user) {
    conf.setProperty(Keys.USER, user);
    return this;
  }

  public String getPassword() {
    return conf.getString(Keys.PASSWORD);
  }

  /**
   * Accumulo password used for authentication.
   * @param password
   * @return
   */
  public AccumuloGraphConfiguration setPassword(byte[] password) {
    return setPassword(new String(password));
  }

  /**
   * Accumulo password used for authentication.
   * @param password
   * @return
   */
  public AccumuloGraphConfiguration setPassword(String password) {
    conf.setProperty(Keys.PASSWORD, password);
    return this;
  }

  public Authorizations getAuthorizations() {
    return conf.containsKey(Keys.AUTHORIZATIONS) ?
        new Authorizations(conf.getString(Keys.AUTHORIZATIONS).getBytes()) : null;
  }

  /**
   * Accumulo authorization permissions.
   * @param auths
   * @return
   */
  public AccumuloGraphConfiguration setAuthorizations(Authorizations auths) {
    byte[] data = auths.getAuthorizationsArray();
    conf.setProperty(Keys.AUTHORIZATIONS, new String(data));
    return this;
  }

  public boolean getSkipExistenceChecks() {
    return conf.getBoolean(Keys.SKIP_CHECKS);
  }

  /**
   * The TinkerPop API defines certain operations should fail if a Vertex or Edge already exists or does not exist. For instance a call to getVertex() must
   * first check if the vertex exists and, if not, return null. Likewise a call to addVertex() must first check if the vertex already exists and if so, throw
   * and exception.
   * <P>
   * However since the AccumuloGraph does not assume (or attempt to maintain) the entire graph in RAM, these checks require a complete round-trip to the
   * Accumulo instance to determine existence.
   * <P>
   * In some instances, the user may decide the existence checks are not needed and would rather not incur the round-trip costs. By setting this flag to true,
   * the AccumuloGraph will not perform existence checks. A request to getVertex() will always return a Vertex instance. It is only when requesting data from
   * that instance the AccumuloGraph will reach out to the Accumulo instance to determine if the Vertex actually exists.
   * <P>
   * In this case it is up to the user's code to ensure that requests for vertices are valid, requests to create new vertices are unique, etc.
   * <P>
   * Note that multiple requests to create the same node does not actually break the AccumuloGraph. The "existence" key/value pair identifying that ID as a
   * Vertex in the vertex table will simply be repeated. The repeated key/value pair will eventually be collapsed back to a single pair (on the next compaction
   * of the Accumulo graph). The trade-off here is additional I/O sending (potentially) duplicate create messages when nodes are repeated versus requiring a
   * round-trip on every node create. In cases where repeated nodes are infrequent, it may be more efficient to simply (re-)create the Vertex rather than find
   * the node in the graph.
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
   *   src = graph.addVertex(srcId);
   * }
   * Vertex dest = graph.getVertex(destID);
   * if (dest == null) {
   *   dest = graph.addVertex(destID);
   * }
   * graph.addEdge(null, src, dest, &quot;myEdge&quot;);
   * </pre>
   * 
   * if you are creating many edges where source and destination edges are infrequently repeated.
   * <P>
   * Similarly, in instances where the application can guarantee that source and destinations
   * vertices for a given new edge have already been added to the
   * Graph, skipping the existence checks for the source and destination
   * vertices and utilizing getVertex() directly can speed the processing.
   * <P>
   * This flag defaults to false (checks will be made).
   * 
   * @param skip
   * @return
   */
  public AccumuloGraphConfiguration setSkipExistenceChecks(boolean skip) {
    conf.setProperty(Keys.SKIP_CHECKS, skip);
    return this;
  }

  public int getPropertyCacheTimeout(String property) {
    if (property != null) {
      property = "." + property;
      int timeout = conf.getInt(Keys.PROPERTY_CACHE_TIMEOUT+property, -1);
      if (timeout >= 0) {
        return timeout;
      }
    }
    return conf.getInt(Keys.PROPERTY_CACHE_TIMEOUT, -1);
  }

  /**
   * Sets the number of milliseconds since retrieval that a property value will
   * be maintained in a RAM cache before that value is expired. If this value is
   * unset or set to 0 (or a negative number) no caching will be performed.
   * <P>
   * A round-trip to Accumulo to retrieve a property value is an expensive operation.
   * Setting this value to a positive number allows the AccumuloGraph to cache
   * retrieved values and use those values (without re-consulting the backing Accumulo
   * store) for the specified time. In situations where the graph is changing
   * slowly and/or properties are revisited frequently, this can achieve a
   * significant reduction in latency at the expense of consistency.
   * <P>
   * If the <tt>property</tt> parameter is null, this sets the default timeout
   * for all properties. Otherwise, only the specified property's timeout
   * is set.
   * <p/>
   * The default is unset (no caching).
   * 
   * @param millis
   *          the maximum number of milliseconds properties can be held in RAM
   * @return
   */
  public AccumuloGraphConfiguration setPropertyCacheTimeout(String property, int millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("Timeout value cannot be negative.");
    }

    if (property != null) {
      property = "."+property;
      if (millis <= 0) {
        conf.clearProperty(Keys.PROPERTY_CACHE_TIMEOUT+property);
      } else {
        conf.setProperty(Keys.PROPERTY_CACHE_TIMEOUT+property, millis);
      }
    } else {
      if (millis <= 0) {
        conf.clearProperty(Keys.PROPERTY_CACHE_TIMEOUT);
      } else {
        conf.setProperty(Keys.PROPERTY_CACHE_TIMEOUT, millis);
      }
    }
    return this;
  }

  /**
   * Whether the vertex cache is enabled (i.e., both
   * size and timeout are positive).
   * @return
   */
  public boolean getVertexCacheEnabled() {
    return getVertexCacheSize() > 0 && getVertexCacheTimeout() > 0;
  }

  public int getVertexCacheSize() {
    return conf.getInt(Keys.VERTEX_CACHE_SIZE, -1);
  }

  public int getVertexCacheTimeout() {
    return conf.getInt(Keys.VERTEX_CACHE_TIMEOUT, -1);
  }

  /**
   * Sets the number of milliseconds since retrieval that a Vertex instance will be maintained
   * in a RAM cache before that value is expired. Also set the maximum
   * size of the cache. If these values are
   * unset or set to 0 (or a negative number) no caching will be performed.
   * <P>
   * A round-trip to Accumulo to retrieve a Vertex is an expensive operation.
   * Setting this value to a positive number allows the AccumuloGraph to cache
   * retrieved Vertices and use those references (without re-consulting the backing
   * Accumulo store) for the specified time. In situations where the graph is
   * changing slowly and/or Vertices are revisited frequently, this can achieve a
   * significant reduction in latency at the expense of consistency.
   * <P>
   * The default is unset (no caching).
   * 
   * @param size
   *          maximum size of the cache
   * @param millis
   *          the maximum number of milliseconds a Vertex should be held in RAM
   * @return
   */
  public AccumuloGraphConfiguration setVertexCacheParams(int size, int millis) {
    if ((size <= 0 || millis <= 0) && (size > 0 || millis > 0)) {
      throw new IllegalArgumentException("Parameters must be both non-positive or both positive");
    }

    if (size <= 0) {
      conf.clearProperty(Keys.VERTEX_CACHE_SIZE);
    } else {
      conf.setProperty(Keys.VERTEX_CACHE_SIZE, size);
    }

    if (millis <= 0) {
      conf.clearProperty(Keys.VERTEX_CACHE_TIMEOUT);
    } else {
      conf.setProperty(Keys.VERTEX_CACHE_TIMEOUT, millis);
    }

    return this;
  }

  /**
   * Whether the edge cache is enabled (i.e., both
   * size and timeout are positive).
   * @return
   */
  public boolean getEdgeCacheEnabled() {
    return getEdgeCacheSize() > 0 && getEdgeCacheTimeout() > 0;
  }

  public int getEdgeCacheSize() {
    return conf.getInt(Keys.EDGE_CACHE_SIZE, -1);
  }

  public int getEdgeCacheTimeout() {
    return conf.getInt(Keys.EDGE_CACHE_TIMEOUT, -1);
  }

  /**
   * Sets the number of milliseconds since retrieval that an Edge instance will be maintained
   * in a RAM cache before that value is expired. Also set the maximum size of the cache.
   * If these values are unset or set to 0 (or a negative number) no caching will be performed.
   * <P>
   * A round-trip to Accumulo to retrieve an Edge is an expensive operation.
   * Setting this value to a positive number allows the AccumuloGraph to cache retrieved
   * Edges and use those references (without re-consulting the backing Accumulo store)
   * for the specified time. In situations where the graph is changing slowly
   * and/or Edges are revisited frequently, this can achieve a significant reduction
   * in latency at the expense of consistency.
   * <P>
   * The default is unset (no caching).
   * 
   * @param size
   *          maximum size of the cache
   * @param millis
   *          the maximum number of milliseconds an Edge should be held in RAM
   * @return
   */
  public AccumuloGraphConfiguration setEdgeCacheParams(int size, int millis) {
    if ((size <= 0 || millis <= 0) && (size > 0 || millis > 0)) {
      throw new IllegalArgumentException("Parameters must be both non-positive or both positive");
    }

    if (size <= 0) {
      conf.clearProperty(Keys.EDGE_CACHE_SIZE);
    } else {
      conf.setProperty(Keys.EDGE_CACHE_SIZE, size);
    }

    if (millis <= 0) {
      conf.clearProperty(Keys.EDGE_CACHE_TIMEOUT);
    } else {
      conf.setProperty(Keys.EDGE_CACHE_TIMEOUT, millis);
    }

    return this;
  }

  public int getQueryThreads() {
    return conf.getInt(Keys.QUERY_THREADS);
  }

  /**
   * Number of Accumulo query threads to use.
   * @param threads
   * @return
   */
  public AccumuloGraphConfiguration setQueryThreads(int threads) {
    if (threads < 1) {
      throw new IllegalArgumentException("You must provide at least 1 query thread.");
    }
    conf.setProperty(Keys.QUERY_THREADS, threads);
    return this;
  }

  public ColumnVisibility getColumnVisibility() {
    return new ColumnVisibility(conf.getString(Keys.COLVIS).getBytes());
  }

  /**
   * Column visibility.<p/>
   * <strong>NOTE:</strong> Currently unused.
   * @param colVis
   * @return
   */
  public AccumuloGraphConfiguration setColumnVisibility(ColumnVisibility colVis) {
    conf.setProperty(Keys.COLVIS, new String(colVis.flatten()));
    return this;
  }

  public boolean getAutoIndex() {
    Object bool = conf.getProperty(Keys.AUTO_INDEX);
    if (bool == null)
      return false;
    return (Boolean) bool;
  }

  /**
   * Whether to automatically index element properties.
   * @param enable
   * @return
   */
  public AccumuloGraphConfiguration setAutoIndex(boolean enable) {
    conf.setProperty(Keys.AUTO_INDEX, enable);
    return this;
  }

  public boolean getIndexableGraphDisabled() {
    Object bool = conf.getProperty(Keys.DISABLE_INDEX);
    if (bool == null)
      return false;
    return (Boolean) bool;
  }

  /**
   * Disables the {@link IndexableGraph} functions.
   * Turning it off will improve the performance of removing elements
   * Note: This disables IndexableGraph, not {@link KeyIndexableGraph}.
   * @param disable
   * @return this configuration
   */
  public AccumuloGraphConfiguration setIndexableGraphDisabled(boolean disable) {
    conf.setProperty(Keys.DISABLE_INDEX, disable);
    return this;
  }

  public SortedSet<Text> getSplits() {
    String[] val = conf.getStringArray(Keys.SPLITS);
    if ((val == null) || (val.length == 0)) {
      return null;
    }
    SortedSet<Text> splits = new TreeSet<Text>();
    for (String s : val) {
      splits.add(new Text(s));
    }
    return splits;
  }

  /**
   * A space-separated, ordered list of splits to be applied
   * to the backing Accumulo table. The splits are only applied if the
   * graph does not already exist and {@link #setCreate(boolean)}
   * is true.
   * @param splits
   * @return
   */
  public AccumuloGraphConfiguration setSplits(String splits) {
    if (splits == null || splits.trim().isEmpty()) {
      return this;
    }
    return setSplits(splits.trim().split(" "));
  }

  /**
   * Set table splits (see {@link #setSplits(String)})
   * but specified as an array of splits.
   * @param splits
   * @return
   */
  public AccumuloGraphConfiguration setSplits(String[] splits) {
    conf.setProperty(Keys.SPLITS, splits != null ? Arrays.asList(splits) : null);
    return this;
  }

  public boolean getAutoFlush() {
    return conf.getBoolean(Keys.AUTO_FLUSH);
  }

  /**
   * Whether updates should be immediately flushed to the backing
   * Accumulo store (true) or not (false). The TinkerPop API expects immediate
   * consistency requiring each individual update to be immediately
   * flushed to Accumulo.
   * However, this incurs significant overhead. For applications at scale
   * that do not require immediate consistency, this flag allows the user
   * to lessen the TinkerPop restriction.<p/>
   * This is enabled by default to support the expected TinkerPop behavior.
   * However, it is strongly recommended that this option be disabled.
   * 
   * @param autoFlush
   * @return
   */
  public AccumuloGraphConfiguration setAutoFlush(boolean autoFlush) {
    conf.setProperty(Keys.AUTO_FLUSH, autoFlush);
    return this;
  }

  public String getGraphName() {
    return conf.getString(Keys.GRAPH_NAME);
  }

  /**
   * Name of the graph to create. Storage tables will be prefixed with this value.
   * <p/>Note: Accumulo only allows table names with alphanumeric and underscore
   * characters.
   * @param name
   * @return
   */
  public AccumuloGraphConfiguration setGraphName(String name) {
    if (!isValidGraphName(name)) {
      throw new IllegalArgumentException("Invalid graph name."
          + " Only alphanumerics and underscores are allowed");
    }

    conf.setProperty(Keys.GRAPH_NAME, name);
    return this;
  }

  /**
   * Make sure this is a valid graph name because of restrictions
   * on table names.
   * @param name
   */
  private static boolean isValidGraphName(String name) {
    return name.matches("^[A-Za-z0-9_]+$");
  }

  public boolean getPreloadAllProperties() {
    return conf.getBoolean(Keys.PRELOAD_ALL_PROPERTIES);
  }

  /**
   * If true, retrieve all properties for elements when
   * retrieving them from Accumulo. This can be used
   * in lieu of {@link #setPreloadedProperties(String[])}
   * when all properties are needed. Defaults to false.
   * @param preload
   * @return
   */
  public AccumuloGraphConfiguration setPreloadAllProperties(boolean preload) {
    conf.setProperty(Keys.PRELOAD_ALL_PROPERTIES, preload);
    return this;
  }

  public String[] getPreloadedProperties() {
    return conf.containsKey(Keys.PRELOADED_PROPERTIES) ?
        conf.getStringArray(Keys.PRELOADED_PROPERTIES) : null;
  }

  /**
   * A trip to the backing-Accumulo store to obtain data is a relatively expensive operation.
   * In cases where the end-user knows there are certain sets of
   * properties that will always/very likely be obtained, it may be more efficient to
   * grab all of those properties at once as the element existence is
   * confirmed. In other cases (e.g., rarely used or very large properties)
   * it may be more efficient to wait to obtain the data until the program determines the
   * property is in fact needed.
   * <P>
   * Deferred property loading is the default. By setting this configuration
   * value, any keys in the provided property list will be automatically loaded in bulk
   * when it makes sense (i.e., when the system has to make a trip out to Accumulo
   * anyway). Other proerties not in the list will continue to be lazily and
   * individually loaded.
   * <P>
   * In order to set this value, you must first define a positive property
   * cache timeout value ({@link #setPropertyCacheTimeout(String, int)}; it does not make sense to
   * pre-load data if you do not allow caching.
   * 
   * @param propertyKeys
   * @return
   */
  public AccumuloGraphConfiguration setPreloadedProperties(String[] propertyKeys) {
    if (propertyKeys == null) {
      throw new NullPointerException("Property keys cannot be null.");
    }

    conf.setProperty(Keys.PRELOADED_PROPERTIES, propertyKeys);
    return this;
  }

  public String[] getPreloadedEdgeLabels() {
    return conf.containsKey(Keys.PRELOAD_EDGES) ?
        conf.getStringArray(Keys.PRELOAD_EDGES) : null;
  }

  /**
   * Labels of graph edges to preload when fetching elements.<p/>
   * <strong>TODO</strong> Currently unused.
   * @param edgeLabels
   * @return
   */
  public AccumuloGraphConfiguration setPreloadedEdgeLabels(String[] edgeLabels) {
    if (edgeLabels == null) {
      throw new NullPointerException("Edge labels cannot be null.");
    }

    Integer timeout = getEdgeCacheTimeout();
    if (timeout == null) {
      throw new IllegalArgumentException("You cannot preload edges " + "without first setting #edgeCacheTimeout(int millis) " + "to a positive value.");
    }

    conf.setProperty(Keys.PRELOAD_EDGES, edgeLabels);
    return this;
  }

  public long getMaxWriteLatency() {
    return conf.getLong(Keys.MAX_WRITE_LATENCY);
  }

  /**
   * Maximum wait time before changes are flushed to Accumulo (milliseconds).
   * @param millis
   * @return
   */
  public AccumuloGraphConfiguration setMaxWriteLatency(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("Maximum write latency must be a positive number, " + "or '0' for no maximum.");
    }
    conf.setProperty(Keys.MAX_WRITE_LATENCY, millis);
    return this;
  }

  public long getMaxWriteMemory() {
    return conf.getLong(Keys.MAX_WRITE_MEMORY);
  }

  /**
   * Maximum memory usage when buffering writes.
   * @param mem
   * @return
   */
  public AccumuloGraphConfiguration setMaxWriteMemory(long mem) {
    if (mem <= 0) {
      throw new IllegalArgumentException("Maximum write memory must be a positive number.");
    }
    conf.setProperty(Keys.MAX_WRITE_MEMORY, mem);
    return this;
  }

  public int getMaxWriteThreads() {
    return conf.getInt(Keys.MAX_WRITE_THREADS);
  }

  /**
   * Maximum number of threads to use for writing to Accumulo.
   * @param threads
   * @return
   */
  public AccumuloGraphConfiguration setMaxWriteThreads(int threads) {
    if (threads < 1) {
      throw new IllegalArgumentException("Maximum write threads must be positive.");
    }
    conf.setProperty(Keys.MAX_WRITE_THREADS, threads);
    return this;
  }

  public long getMaxWriteTimeout() {
    return conf.getLong(Keys.MAX_WRITE_TIMEOUT);
  }

  /**
   * How long to wait before declaring a write failure (milliseconds).
   * @param millis
   * @return
   */
  public AccumuloGraphConfiguration setMaxWriteTimeout(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("Maximum write timeout must be a positive number, "
          + "or '0' for no maximum.");
    }
    conf.setProperty(Keys.MAX_WRITE_TIMEOUT, millis);
    return this;
  }

  /**
   * Create a {@link Connector} from this configuration.
   * <p/>Note: Once this is called, the configuration may not be modified.
   * @return
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   * @throws IOException
   * @throws InterruptedException
   */
  public Connector getConnector() throws AccumuloException, AccumuloSecurityException,
  IOException, InterruptedException {
    if (connector == null) {
      Instance inst = null;
      switch (getInstanceType()) {
        case Distributed:
          if (getInstanceName() == null) {
            throw new IllegalArgumentException("Must specify instance name for distributed mode");
          } else if (getZooKeeperHosts() == null) {
            throw new IllegalArgumentException("Must specify ZooKeeper hosts for distributed mode");
          }
          inst = new ZooKeeperInstance(getInstanceName(), getZooKeeperHosts());
          break;

        case Mini:
          File dir = null;
          if (miniClusterTempDir == null) {
            dir = createTempDir();
            dir.deleteOnExit();
          } else {
            // already set by setMiniClusterTempDir(), It should be cleaned up outside of this class.
            dir = new File(miniClusterTempDir);
          }
          accumuloMiniCluster = new MiniAccumuloCluster(dir, ""); // conf.getString(PASSWORD)
          try {
            accumuloMiniCluster.start();
          } catch (Exception ex) {
            throw new AccumuloGraphException(ex);
          }
          inst = new ZooKeeperInstance(accumuloMiniCluster.getInstanceName(), accumuloMiniCluster.getZooKeepers());
          throw new UnsupportedOperationException("TODO");

        case Mock:
          inst = new MockInstance(getInstanceName());
          break;

        default:
          throw new AccumuloGraphException("Unexpected instance type: " + inst);
      }

      connector = inst.getConnector(getUser(), new PasswordToken(getPassword()));

      // Make the configuration immutable.
      conf.addConfigurationListener(new ConfigurationListener() {
        @Override
        public void configurationChanged(ConfigurationEvent event) {
          throw new AccumuloGraphException("You may not modify the configuration after calling getConnector()");
        }
      });
    }

    return connector;
  }

  /**
   * Create a {@link BatchWriterConfig} based on this configuration.
   * @return
   */
  public BatchWriterConfig getBatchWriterConfig() {
    return new BatchWriterConfig().setMaxLatency(getMaxWriteLatency(), TimeUnit.MILLISECONDS)
        .setMaxMemory(getMaxWriteMemory())
        .setMaxWriteThreads(getMaxWriteThreads())
        .setTimeout(getMaxWriteTimeout(), TimeUnit.MILLISECONDS);
  }

  /**
   * File-based version of {@link #setMiniClusterTempDir(String)}.
   * @param miniClusterTempDir
   */
  public AccumuloGraphConfiguration setMiniClusterTempDir(File miniClusterTempDir) {
    return setMiniClusterTempDir(miniClusterTempDir.getPath());
  }

  /**
   * Used by JUnit Tests to set the miniClusterTempDirectory.
   * If not set in advance of a test, getConnector will use a
   * Java Temporary Folder which will not be deleted afterwards.
   * 
   * @param miniClusterTempDir
   */
  public AccumuloGraphConfiguration setMiniClusterTempDir(String miniClusterTempDir) {
    this.miniClusterTempDir = miniClusterTempDir;
    return this;
  }

  /**
   * Name of vertex table (keyed by vertex id).
   * @return
   */
  public String getVertexTableName() {
    return getGraphName() + "_vertex";
  }

  /**
   * Name of edge table (keyed by edge id).
   * @return
   */
  public String getEdgeTableName() {
    return getGraphName() + "_edge";
  }

  /**
   * Name of vertex key index table (keyed on
   * vertex property keys).
   * @return
   */
  public String getVertexKeyIndexTableName() {
    return getGraphName() + "_vertex_key_index";
  }

  /**
   * Name of edge key index table (keyed on
   * edge property keys).
   * @return
   */
  public String getEdgeKeyIndexTableName() {
    return getGraphName() + "_edge_key_index";
  }

  /**
   * Table of the index with given name (keyed
   * on property keys of the given element type).
   * @param indexName
   * @return
   */
  public String getNamedIndexTableName(String indexName) {
    return getGraphName() + "_index_" + indexName;
  }

  /**
   * Table containing index metadata.
   * @return
   */
  public String getIndexMetadataTableName() {
    return getGraphName() + "_index_metadata";
  }

  List<String> getTableNames() {
    return Arrays.asList(getVertexTableName(),
        getEdgeTableName(), getVertexKeyIndexTableName(), getEdgeKeyIndexTableName(),
        getIndexMetadataTableName());
  }

  /**
   * Ensure that required properties are set for this
   * configuration.
   */
  public void validate() {

    switch (getInstanceType()) {
      case Distributed:
        checkPropertyValue(Keys.ZK_HOSTS, getZooKeeperHosts(), false);
        checkPropertyValue(Keys.USER, getUser(), false);
        checkPropertyValue(Keys.PASSWORD, getPassword(), false);
        // no break intentional
      case Mini:
        checkPropertyValue(Keys.INSTANCE, getInstanceName(), false);
        checkPropertyValue(Keys.PASSWORD, getPassword(), true);
        // no break intentional
      case Mock:
        checkPropertyValue(Keys.GRAPH_NAME, getGraphName(), false);
        break;
      default:
        throw new AccumuloGraphException("Unexpected instance type: " + getInstanceType());
    }

    int timeout = getPropertyCacheTimeout(null);

    if (timeout <= 0) {
      String[] props = conf.getStringArray(Keys.PRELOADED_PROPERTIES);
      for (int i = 0; i < props.length; i++) {
        timeout = getPropertyCacheTimeout(props[i]);
        if (timeout <= 0) {
          break;
        }
      }
    }

    if (getPreloadAllProperties() && getPreloadedProperties() != null) {
      throw new IllegalArgumentException("Cannot preload all properties"
          + " and specified properties simultaneously");
    }

    if (timeout <= 0 && (getPreloadedProperties() != null || getPreloadAllProperties())) {
      throw new IllegalArgumentException("You cannot preload properties "
          + "without first setting #propertyCacheTimeout(String property, int millis) "
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

  private File createTempDir() throws IOException {
    File temp = File.createTempFile(AccumuloGraphConfiguration
        .class.getSimpleName(), ".mini.tmp");
    Files.delete(temp.toPath());
    Files.createDirectory(temp.toPath());
    return temp;
  }

  /**
   * Print out this configuration.
   */
  public void print() {
    System.out.println(AccumuloGraphConfiguration.class+":");

    for (String key : getValidInternalKeys()) {
      String value = "(null)";
      if (conf.containsKey(key)) {
        value = conf.getProperty(key).toString();
      }
      System.out.println("  "+key+" = "+value);
    }
  }

  /**
   * Get the AccumuloGraph-specific keys for this configuration.
   * @return
   */
  static Set<String> getValidInternalKeys() {
    Set<String> keys = new TreeSet<String>();

    for (Field field : Keys.class.getDeclaredFields()) {
      try {
        keys.add((String) field.get(null));
      } catch (Exception e) {
        throw new AccumuloGraphException(e);
      }
    }

    return keys;
  }

  // Old deprecated method names.

  /**
   * @deprecated This is an old method name. Use {@link #setZooKeeperHosts(String)}.
   * @param zookeeperHosts
   * @return
   */
  @Deprecated
  public AccumuloGraphConfiguration setZookeeperHosts(String zookeeperHosts) {
    return setZooKeeperHosts(zookeeperHosts);
  }

  /**
   * @deprecated This is an old method name. Use {@link #setSkipExistenceChecks(boolean)}.
   * @param skip
   * @return
   */
  @Deprecated
  public AccumuloGraphConfiguration skipExistenceChecks(boolean skip) {
    return setSkipExistenceChecks(skip);
  }

  /**
   * @deprecated This is an old method name. Use {@link #getAutoIndex()}.
   * @return
   */
  @Deprecated
  public boolean isAutoIndex() {
    return getAutoIndex();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getIndexableGraphDisabled()}.
   * @return
   */
  @Deprecated
  public boolean isIndexableGraphDisabled() {
    return getIndexableGraphDisabled();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getCreate()}.
   * @return
   */
  @Deprecated
  public boolean isCreate() {
    return getCreate();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getClear()}.
   * @return
   */
  @Deprecated
  public boolean isClear() {
    return getClear();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getInstanceName()}.
   * @return
   */
  @Deprecated
  public String getInstance() {
    return getInstanceName();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getAutoFlush()}.
   * @return
   */
  @Deprecated
  public boolean isAutoFlush() {
    return getAutoFlush();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getPropertyCacheTimeout(String)}.
   * @return
   */
  @Deprecated
  public Integer getPropertyCacheTimeoutMillis(String property) {
    return getPropertyCacheTimeout(property);
  }

  /**
   * @deprecated This is an old method name. Use {@link #getEdgeCacheTimeout()}.
   * @return
   */
  @Deprecated
  public Integer getEdgeCacheTimeoutMillis() {
    return getEdgeCacheTimeout();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getVertexCacheTimeout()}.
   * @return
   */
  @Deprecated
  public Integer getVertexCacheTimeoutMillis() {
    return getVertexCacheTimeout();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getSkipExistenceChecks()}.
   * @return
   */
  @Deprecated
  public boolean skipExistenceChecks() {
    return getSkipExistenceChecks();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getGraphName()}.
   * @return
   */
  @Deprecated
  public String getName() {
    return getGraphName();
  }

  /**
   * @deprecated This is an old method name. Use {@link #getPreloadedEdgeLabels()}.
   * @return
   */
  @Deprecated
  public String[] getPreloadedEdges() {
    return getPreloadedEdgeLabels();
  }


  // Abstract methods from the AbstractConfiguration implementation.

  @Override
  public boolean isEmpty() {
    return conf.isEmpty();
  }

  @Override
  public boolean containsKey(String key) {
    return conf.containsKey(key);
  }

  @Override
  public Object getProperty(String key) {
    return conf.getProperty(key);
  }

  @Override
  public Iterator<String> getKeys() {
    return conf.getKeys();
  }

  @Override
  protected void addPropertyDirect(String key, Object value) {
    // Only allow AccumuloGraph-specific keys.
    if (getValidInternalKeys().contains(key)) {
      conf.setProperty(key, value);
    } else {
      throw new UnsupportedOperationException("Invalid key: "+key);
    }
  }
}
