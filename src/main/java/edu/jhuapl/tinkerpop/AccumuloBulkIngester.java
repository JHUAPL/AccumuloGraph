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

import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.edge.EdgeEndpointsMutator;
import edu.jhuapl.tinkerpop.mutator.edge.EdgeMutator;
import edu.jhuapl.tinkerpop.mutator.property.WritePropertyMutator;
import edu.jhuapl.tinkerpop.mutator.vertex.AddVertexMutator;


/**
 * This class provides high-speed ingest into an {@link AccumuloGraph} instance
 * in exchange for consistency guarantees. That is, users of this class must ensure
 * (outside of this class) that data is entered in a consistent way or the behavior
 * or the resulting AccumuloGraph is undefined. For example, users are required
 * to ensure that a vertex ID provided as the source or destination of an edge exists
 * (or will exist by the end of the ingest process). Likewise, it is the
 * user's responsibility to ensure vertex and edge IDs provided for properties (will) exist.
 * <P>
 * TODO define the properties that will be used (vs. those that are ignored) from the provided AccumuloGraphConfiguration.
 * 
 */
public final class AccumuloBulkIngester {

  /**
   * The connector to the backing Accumulo instance.
   */
  Connector connector;

  /**
   * User-provided configuration details.
   */
  AccumuloGraphConfiguration config;

  /**
   * Parent MTBW for writing mutation into Accumulo.
   */
  MultiTableBatchWriter mtbw;

  /**
   * Writer to the vertex table; child of {@link #mtbw}.
   */
  BatchWriter vertexWriter;

  /**
   * Writer to the edge table; child of {@link #mtbw}.
   */
  BatchWriter edgeWriter;

  /**
   * Create an ingester using the given configuration parameters.
   * 
   * @param config
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   * @throws TableExistsException
   * @throws InterruptedException
   * @throws IOException
   */
  public AccumuloBulkIngester(AccumuloGraphConfiguration config) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException, IOException, InterruptedException {
    this.config = config;
    connector = config.getConnector();

    AccumuloGraphUtils.handleCreateAndClear(config);

    mtbw = connector.createMultiTableBatchWriter(config.getBatchWriterConfig());
    vertexWriter = mtbw.getBatchWriter(config.getVertexTableName());
    edgeWriter = mtbw.getBatchWriter(config.getEdgeTableName());
  }

  /**
   * Adds a vertex with the given ID. Returns a PropertyBuilder that can be used to add multiple properties to the newly created vertex. Using the returned
   * property builder to add multiple properties to this vertex will be more efficient than calling {@link #addVertexProperty(String, String, Object)} multiple
   * times as using the PropertyBuilder will result in fewer object creates.
   * <P>
   * No checks are performed to see if the given ID already exists or if it has any attributes or edges already defined. This method simply creates the node
   * (possibly again) in the backing data store.
   * 
   * @param id
   * @return
   * @throws MutationsRejectedException
   */
  public PropertyBuilder addVertex(String id) throws MutationsRejectedException {
    Mutators.apply(vertexWriter, new AddVertexMutator(id));
    return new PropertyBuilder(vertexWriter, id);
  }

  /**
   * Adds the given value as a property using the given key to a vertex with the given id.
   * <P>
   * No checks are performed to ensure the ID is a valid vertex nor to determine if the given key already has a value. The provided value is simply written as
   * the latest value. It is the user's responsibility to ensure before the end of processing that the provided vertex ID exists. It is not, however, a
   * requirement that the ID exist before a call to this method.
   * <P>
   * If you are creating the vertex and adding multiple properties at the same time, consider using the PropertyBuilder returned by {@link #addVertex(String)}.
   * 
   * @param id
   * @param key
   * @param value
   * @throws MutationsRejectedException
   */
  public void addVertexProperty(String id, String key, Object value) throws MutationsRejectedException {
    addProperty(vertexWriter, id, key, value);
  }

  /**
   * Adds an edge with a unique ID. Returns a PropertyBuilder that can be used to add multiple properties to the newly created edge. Using the returned property
   * builder to add multiple properties to this edge will be more efficient than calling {@link #addEdgeProperty(String, String, Object)} multiple times as
   * using the PropertyBuilder will result in fewer object creates.
   * <P>
   * No checks are performed to see if the given source and destination IDs exist as vertices. This method simply creates the edge in the backing data store
   * with a unique ID.
   * 
   * @see #addEdge(String, String, String, String)
   * @param src
   * @param dest
   * @param label
   * @return
   * @throws MutationsRejectedException
   */
  public PropertyBuilder addEdge(String src, String dest, String label) throws MutationsRejectedException {
    return addEdge(UUID.randomUUID().toString(), src, dest, label);
  }

  /**
   * Adds an edge with the given ID. Returns a PropertyBuilder that can be used to add multiple properties to the newly created edge. Using the returned
   * property builder to add multiple properties to this edge will be more efficient than calling {@link #addEdgeProperty(String, String, Object)} multiple
   * times as using the PropertyBuilder will result in fewer object creates.
   * <P>
   * No checks are performed to see if the given source and destination IDs exist as vertices or if the given edge ID already exists. This method simply creates
   * the edge (possibly again) in the backing data store.
   * 
   * @param id
   * @param src
   * @param dest
   * @param label
   * @return
   * @throws MutationsRejectedException
   */
  public PropertyBuilder addEdge(String id, String src, String dest, String label) throws MutationsRejectedException {
    Mutators.apply(edgeWriter, new EdgeMutator.Add(id, src, dest, label));
    Mutators.apply(vertexWriter, new EdgeEndpointsMutator.Add(id, src, dest, label));
    return new PropertyBuilder(edgeWriter, id);
  }

  /**
   * Adds the given value as a property using the given key to an edge with the given id.
   * <P>
   * No checks are performed to ensure the ID is a valid edge nor to determine if the given key already has a value. The provided value is simply written as the
   * latest value. It is the user's responsibility to ensure before the end of processing that the provided edge ID exists. It is not, however, a requirement
   * that the ID exist before a call to this method.
   * <P>
   * If you are creating the edge and adding multiple properties at the same time, consider using the PropertyBuilder returned by
   * {@link #addEdge(String, String, String, String)}.
   * 
   * @param id
   * @param key
   * @param value
   * @throws MutationsRejectedException
   */
  public void addEdgeProperty(String id, String key, Object value) throws MutationsRejectedException {
    addProperty(edgeWriter, id, key, value);
  }

  /**
   * Adds the provided proprty to the given writer.
   * 
   * @param writer
   * @param id
   * @param key
   * @param value
   * @throws MutationsRejectedException
   */
  private void addProperty(BatchWriter writer, String id, String key, Object value) throws MutationsRejectedException {
    Mutators.apply(writer, new WritePropertyMutator(id, key, value));
  }

  /**
   * Shutdown the bulk ingester. This flushes any outstanding writes to Accumulo
   * and performs any remaining clean up to finalize the graph.
   * 
   * @param compact
   *          a flag if this shutdown should start a compaction on the graph-related tables
   *          (true) or not (false) before quitting.
   * @throws AccumuloException
   * @throws TableNotFoundException
   * @throws AccumuloSecurityException
   */
  public void shutdown(boolean compact) throws AccumuloSecurityException,
    TableNotFoundException, AccumuloException {
	// Make sure this wasn't closed already.
    if (mtbw == null) {
      throw new RuntimeException("Ingester was already closed");
    }

    mtbw.close();
    mtbw = null;

    // Disable the "create" and "clear" options so we don't blow away
    // everything we just added.
    AccumuloGraphConfiguration copy = new AccumuloGraphConfiguration(config);
    copy.setCreate(false).setClear(false);

    AccumuloGraph g = (AccumuloGraph) GraphFactory.open(copy.getConfiguration());
    for (String key : g.getIndexedKeys(Vertex.class)) {
      g.dropKeyIndex(key, Vertex.class);
      g.createKeyIndex(key, Vertex.class);
    }

    for (String key : g.getIndexedKeys(Edge.class)) {
      g.dropKeyIndex(key, Edge.class);
      g.createKeyIndex(key, Edge.class);
    }
    g.shutdown();

    // TODO ... other house cleaning/verification?

    if (compact) {
      TableOperations tableOps = connector.tableOperations();
      for (String table : copy.getTableNames()) {
        tableOps.compact(table, null, null, true, false);
      }
    }
  }

  /**
   * A class used to add multiple properties to vertices and edges. This class encapsulates adding multiple properties to a single edge or vertex in a batch in
   * an effort to reduce object creates as part of the persistence operation. Calls to {@link #add(String, Object)} may be chained together.
   * <P>
   * The general use of this object is as follows:
   * 
   * <PRE>
   * PropertyBuilder builder = ingest.addVertex(&quot;MyVertexId&quot;);
   * builder.add(&quot;propertyKey1&quot;, &quot;propertyValue1&quot;).add(&quot;propertyKey2&quot;, &quot;propertyValue2&quot;);
   * builder.add(&quot;propertyKey3&quot;, &quot;propertyValue3&quot;);
   * builder.finish();
   * </PRE>
   */
  public static final class PropertyBuilder {

    final String id;
    final BatchWriter writer;

    PropertyBuilder(BatchWriter writer, String id) {
      this.writer = writer;
      this.id = id;
    }

    /**
     * Add the given property with the given value to the edge or vertex associated with this build. You must call {@link #finish()} when all of the properties
     * have been added in order for these updates to be persisted in Accumulo.
     * 
     * @param key
     * @param value
     * @return
     */
    public PropertyBuilder add(String key, Object value) {
      for (Mutation m : new WritePropertyMutator(id, key, value).create()) {
        try {
          writer.addMutation(m);
        } catch (MutationsRejectedException e) {
          throw new AccumuloGraphException(e);
        }
      }
      return this;
    }

    /**
     * Called to write all properties added to this builder out to Accumulo.
     * 
     * @throws MutationsRejectedException
     */
    public void finish() throws MutationsRejectedException {
      // No-op since Mutations are now added on the fly.
    }

    /**
     * Returns the vertex or edge ID associated with this builder.
     * 
     * @return
     */
    public String getId() {
      return id;
    }
  }
}
