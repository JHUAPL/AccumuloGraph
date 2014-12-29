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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * 
 * This is an implementation of TinkerPop's graph API
 * backed by Apache Accumulo. In addition to the basic
 * Graph interface, the implementation
 * supports {@link IndexableGraph} and {@link KeyIndexableGraph}.
 * 
 * <p/>Tables have the following formats.
 * 
 * <p/>
 * <table border="1">
 *  <caption>Vertex table</caption>
 *  <thead>
 *    <tr><th>ROWID</th><th>COLFAM</th><th>COLQUAL</th><th>VALUE</th></tr>
 *  </thead>
 *  <tbody>
 *    <tr><td>VertexID</td><td>LABEL</td><td>EXISTS</td><td>[empty]</td></tr>
 *    <tr><td>VertexID</td><td>INEDGE</td><td>InVertexID_EdgeID</td><td>EdgeLabel</td></tr>
 *    <tr><td>VertexID</td><td>OUTEDGE</td><td>OutVertexID_EdgeID</td><td>EdgeLabel</td></tr>
 *    <tr><td>VertexID</td><td>PropertyKey</td><td>[empty]</td><td>Encoded Value</td></tr>
 *  </tbody>
 * </table>
 * 
 * <p/>
 * <table border="1">
 *  <caption>Edge table</caption>
 *  <thead>
 *    <tr> <th>ROWID</th><th>COLFAM</th><th>COLQUAL</th><th>VALUE</th></tr>
 *  </thead>
 *  <tbody>
 *    <tr><td>EdgeID</td><td>LABEL</td><td>[empty]</td><td>Encoded LabelValue</td></tr>
 *    <tr><td>EdgeID</td><td>INEDGE</td><td>InVertexID</td><td>[empty]</td></tr>
 *    <tr><td>EdgeID</td><td>OUTEDGE</td><td>OutVertexID</td><td>[empty]</td></tr>
 *    <tr><td>EdgeID</td><td>PropertyKey</td><td>[empty]</td><td>Encoded Value</td></tr>
 *  </tbody>
 * </table>
 * 
 * <p/>
 * <table border="1">
 *  <caption>Vertex / edge index tables (each index gets its own table)</caption>
 *  <thead>
 *    <tr><th>ROWID</th><th>COLFAM</th><th>COLQUAL</th><th>VALUE</th></tr>
 *  </thead>
 *  <tbody>
 *    <tr><td>Encoded Value</td><td>PropertyKey</td><td>ElementID</td><td>[empty]</td></tr>
 *  </tbody>
 * </table>
 * 
 * <p/>
 * <table border="1">
 *  <caption>Metadata/key metadata tables</caption>
 *  <thead>
 *    <tr><th>ROWID</th><th>COLFAM</th><th>COLQUAL</th><th>VALUE</th></tr>
 *  </thead>
 *  <tbody>
 *    <tr><td>IndexName</td><td>IndexClassType</td><td>[empty]</td><td>[empty]</td></tr>
 *  </tbody>
 * </table>
 */
public class AccumuloGraph implements Graph, KeyIndexableGraph, IndexableGraph {

  protected AccumuloGraphConfiguration config;

  static byte[] EMPTY = new byte[0];

  public static final String IDDELIM = "_";
  public static final String SLABEL = "L";
  public static final String SINEDGE = "I";
  public static final String SOUTEDGE = "O";
  public static final String SEXISTS = "E";
  public static final byte[] EXISTS = SEXISTS.getBytes();
  public static final byte[] LABEL = SLABEL.getBytes();
  public static final byte[] INEDGE = SINEDGE.getBytes();
  public static final byte[] OUTEDGE = SOUTEDGE.getBytes();
  protected static final Text TEXISTS = new Text(EXISTS);
  protected static final Text TINEDGE = new Text(INEDGE);
  protected static final Text TOUTEDGE = new Text(OUTEDGE);
  protected static final Text TLABEL = new Text(LABEL);

  MultiTableBatchWriter writer;
  BatchWriter vertexBW;
  BatchWriter edgeBW;

  ElementCache<Vertex> vertexCache;
  ElementCache<Edge> edgeCache;

  public AccumuloGraph(Configuration cfg) {
    this(new AccumuloGraphConfiguration(cfg));
  }

  /**
   * Constructor that ensures that the needed tables are made
   * 
   * @param config
   */
  public AccumuloGraph(AccumuloGraphConfiguration config) {
    config.validate();
    this.config = config;

    if (config.getVertexCacheEnabled()) {
      vertexCache = new ElementCache<Vertex>(config.getVertexCacheSize(),
          config.getVertexCacheTimeout());
    }

    if (config.getEdgeCacheEnabled()) {
      edgeCache = new ElementCache<Edge>(config.getEdgeCacheSize(),
          config.getEdgeCacheTimeout());
    }

    AccumuloGraphUtils.handleCreateAndClear(config);
    try {
      setupWriters();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  private void setupWriters() throws Exception {
    writer = config.getConnector().createMultiTableBatchWriter(config.getBatchWriterConfig());

    vertexBW = writer.getBatchWriter(config.getVertexTable());
    edgeBW = writer.getBatchWriter(config.getEdgeTable());
  }

  /**
   * Factory method for GraphFactory
   */
  public static AccumuloGraph open(Configuration properties) throws AccumuloException {
    return new AccumuloGraph(properties);
  }

  protected Scanner getElementScanner(Class<? extends Element> type) {
    try {
      String tableName = config.getEdgeTable();
      if (type.equals(Vertex.class))
        tableName = config.getVertexTable();
      return config.getConnector().createScanner(tableName, config.getAuthorizations());
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  protected Scanner getScanner(String tablename) {
    try {
      return config.getConnector().createScanner(tablename, config.getAuthorizations());
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    } catch (AccumuloException e) {
      e.printStackTrace();
    } catch (AccumuloSecurityException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  // Aliases for the lazy
  protected Scanner getMetadataScanner() {
    return getScanner(config.getMetadataTable());
  }

  protected Scanner getVertexIndexScanner() {
    return getScanner(config.getVertexIndexTable());
  }

  protected Scanner getEdgeIndexScanner() {
    return getScanner(config.getEdgeIndexTable());
  }

  protected BatchWriter getVertexIndexWriter() {
    return getWriter(config.getVertexIndexTable());
  }

  protected BatchWriter getMetadataWriter() {
    return getWriter(config.getMetadataTable());
  }

  protected BatchWriter getEdgeIndexWriter() {
    return getWriter(config.getEdgeIndexTable());
  }

  private Scanner getKeyMetadataScanner() {

    return getScanner(config.getMetadataTable() + "KEY");
  }

  protected BatchWriter getKeyMetadataWriter() {
    return getWriter(config.getMetadataTable() + "KEY");
  }

  public BatchWriter getWriter(String tablename) {
    try {
      return writer.getBatchWriter(tablename);
    } catch (AccumuloException e) {
      e.printStackTrace();
    } catch (AccumuloSecurityException e) {
      e.printStackTrace();
    } catch (TableNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  private BatchScanner getElementBatchScanner(Class<? extends Element> type) {
    try {
      String tableName = config.getVertexTable();
      if (type.equals(Edge.class))
        tableName = config.getEdgeTable();
      BatchScanner x = config.getConnector().createBatchScanner(tableName, config.getAuthorizations(), config.getQueryThreads());
      x.setRanges(Collections.singletonList(new Range()));
      return x;
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
  }

  // End Aliases

  @Override
  public Features getFeatures() {
    return AccumuloFeatures.get();
  }

  @Override
  public Vertex addVertex(Object id) {
    String myID;
    if (id == null) {
      myID = UUID.randomUUID().toString();
    } else {
      try {
        myID = id.toString();// (String) id;
      } catch (ClassCastException e) {
        return null;
      }
    }

    Vertex vert = null;
    if (!config.getSkipExistenceChecks()) {
      vert = getVertex(myID);
      if (vert != null) {
        throw ExceptionFactory.vertexWithIdAlreadyExists(myID);
      }
    }

    Mutation m = new Mutation(myID);
    m.put(LABEL, EXISTS, EMPTY);

    try {
      vertexBW.addMutation(m);
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
      return null;
    }

    checkedFlush();
    vert = new AccumuloVertex(this, myID);

    if (vertexCache != null) {
      vertexCache.cache(vert);
    }
    return vert;
  }

  @Override
  public Vertex getVertex(Object id) {
    if (id == null) {
      throw ExceptionFactory.vertexIdCanNotBeNull();
    }
    String myID = id.toString();

    if (vertexCache != null) {
      Vertex vertex = vertexCache.retrieve(myID);
      if (vertex != null) {
        return vertex;
      }
    }

    Vertex vertex = new AccumuloVertex(this, myID);

    Scanner scan = null;
    try {
      if (!config.getSkipExistenceChecks()) {
        // in addition to just an "existence" check, we will also load
        // any "preloaded" properties now, which saves us a round-trip
        // to Accumulo later.
        scan = getElementScanner(Vertex.class);
        scan.setRange(new Range(myID));
        scan.fetchColumn(TLABEL, TEXISTS);

        String[] preload = config.getPreloadedProperties();
        if (preload != null) {
          // user has requested specific properties.
          Text colf = new Text("");
          for (String key : preload) {
            if (StringFactory.LABEL.equals(key)) {
              colf.set(AccumuloGraph.LABEL);
            } else {
              colf.set(key);
            }
            scan.fetchColumnFamily(colf);
          }
        }

        Iterator<Entry<Key,Value>> iter = scan.iterator();
        if (!iter.hasNext()) {
          return null;
        }

        preloadProperties(iter, (AccumuloElement) vertex);

      }
    } finally {
      if (scan != null) {
        scan.close();
      }
    }

    if (vertexCache != null) {
      vertexCache.cache(vertex);
    }
    return vertex;
  }

  @Override
  public void removeVertex(Vertex vertex) {
    if (vertexCache != null) {
      vertexCache.remove(vertex.getId());
    }
    if (!config.getIndexableGraphDisabled())
      clearIndex(vertex.getId());

    Scanner scan = getElementScanner(Vertex.class);
    scan.setRange(new Range(vertex.getId().toString()));

    BatchDeleter edgedeleter = null;
    BatchDeleter vertexdeleter = null;
    BatchWriter indexdeleter = getVertexIndexWriter();
    try {
      // Set up Deleters
      edgedeleter = config.getConnector().createBatchDeleter(config.getEdgeTable(), config.getAuthorizations(), config.getQueryThreads(),
          config.getBatchWriterConfig());
      vertexdeleter = config.getConnector().createBatchDeleter(config.getVertexTable(), config.getAuthorizations(), config.getQueryThreads(),
          config.getBatchWriterConfig());
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }
    Iterator<Entry<Key,Value>> iter = scan.iterator();
    List<Range> ranges = new ArrayList<Range>();
    if (!iter.hasNext()) {
      throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId());
    }
    try {
      // Search for edges
      while (iter.hasNext()) {
        Entry<Key,Value> e = iter.next();
        Key k = e.getKey();

        if (k.getColumnFamily().equals(TOUTEDGE) || k.getColumnFamily().equals(TINEDGE)) {
          ranges.add(new Range(k.getColumnQualifier().toString().split(IDDELIM)[1]));

          Mutation vm = new Mutation(k.getColumnQualifier().toString().split(IDDELIM)[0]);
          vm.putDelete(invert(k.getColumnFamily()), new Text(vertex.getId().toString() + IDDELIM + k.getColumnQualifier().toString().split(IDDELIM)[1]));
          vertexBW.addMutation(vm);
        } else {
          Mutation m = new Mutation(e.getValue().get());
          m.putDelete(k.getColumnFamily(), k.getRow());
          indexdeleter.addMutation(m);
        }

      }
      checkedFlush();
      scan.close();

      // If Edges are found, delete the whole row
      if (!ranges.isEmpty()) {
        // TODO don't we also have to propagate these deletes to the
        // vertex index table?
        edgedeleter.setRanges(ranges);
        edgedeleter.delete();
        ranges.clear();
      }
      // Delete the whole vertex row
      ranges.add(new Range(vertex.getId().toString()));
      vertexdeleter.setRanges(ranges);
      vertexdeleter.delete();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (edgedeleter != null)
        edgedeleter.close();
      if (vertexdeleter != null)
        vertexdeleter.close();
    }
  }

  // Maybe an Custom Iterator could make this better.
  private void clearIndex(Object id) {
    Iterable<Index<? extends Element>> it = this.getIndices();
    Iterator<Index<? extends Element>> iter = it.iterator();
    while (iter.hasNext()) {
      AccumuloIndex<?> in = (AccumuloIndex<?>) iter.next();
      String table = in.tableName;

      BatchDeleter del = null;
      try {
        del = config.getConnector().createBatchDeleter(table, config.getAuthorizations(), config.getMaxWriteThreads(), config.getBatchWriterConfig());
        del.setRanges(Collections.singleton(new Range()));
        StringBuilder regex = new StringBuilder();
        regex.append(".*\\Q").append(id.toString()).append("\\E$");

        IteratorSetting is = new IteratorSetting(10, "getEdgeFilter", RegExFilter.class);
        RegExFilter.setRegexs(is, null, null, regex.toString(), null, false);
        del.addScanIterator(is);
        del.delete();
      } catch (Exception e) {
        throw new AccumuloGraphException(e);
      } finally {
        if (del != null)
          del.close();
      }
    }
  }

  private Text invert(Text columnFamily) {
    if (columnFamily.toString().equals(INEDGE)) {
      return TOUTEDGE;
    }
    return TINEDGE;
  }

  @Override
  public Iterable<Vertex> getVertices() {
    Scanner scan = getElementScanner(Vertex.class);
    scan.fetchColumnFamily(TLABEL);

    if (config.getPreloadedProperties() != null) {
      for (String x : config.getPreloadedProperties()) {
        scan.fetchColumnFamily(new Text(x));
      }
    }
    return new ScannerIterable<Vertex>(this, scan) {

      @Override
      public Vertex next(PeekingIterator<Entry<Key,Value>> iterator) {
        // TODO could also check local cache before creating a new instance?
        AccumuloVertex vert = new AccumuloVertex(AccumuloGraph.this, iterator.peek().getKey().getRow().toString());

        String rowid = iterator.next().getKey().getRow().toString();
        List<Entry<Key,Value>> vals = new ArrayList<Entry<Key,Value>>();
        while (iterator.peek() != null && rowid.compareToIgnoreCase(iterator.peek().getKey().getRow().toString()) == 0) {
          vals.add(iterator.next());
        }
        preloadProperties(vals.iterator(), vert);
        return vert;
      }
    };
  }

  @Override
  public Iterable<Vertex> getVertices(String key, Object value) {
    checkProperty(key, value);
    if (config.getAutoIndex() || getIndexedKeys(Vertex.class).contains(key)) {
      // Use the index
      Scanner s = getVertexIndexScanner();
      byte[] val = AccumuloByteSerializer.serialize(value);
      Text tVal = new Text(val);
      s.setRange(new Range(tVal, tVal));
      s.fetchColumnFamily(new Text(key));

      return new ScannerIterable<Vertex>(this, s) {

        @Override
        public Vertex next(PeekingIterator<Entry<Key,Value>> iterator) {

          Key key = iterator.next().getKey();
          AccumuloVertex v = null;
          if (vertexCache != null) {
            v = (AccumuloVertex) vertexCache.retrieve(key.getColumnQualifier().toString());
          }

          v = (v == null ? new AccumuloVertex(AccumuloGraph.this, key.getColumnQualifier().toString()) : v);
          v.cacheProperty(key.getColumnFamily().toString(),
              AccumuloByteSerializer.deserialize(key.getRow().getBytes()));

          if (vertexCache != null) {
            vertexCache.cache(v);
          }
          return v;

        }
      };
    } else {
      byte[] val = AccumuloByteSerializer.serialize(value);
      if (val[0] != AccumuloByteSerializer.SERIALIZABLE) {
        BatchScanner scan = getElementBatchScanner(Vertex.class);
        scan.fetchColumnFamily(new Text(key));

        IteratorSetting is = new IteratorSetting(10, "filter", RegExFilter.class);
        RegExFilter.setRegexs(is, null, null, null, Pattern.quote(new String(val)), false);
        scan.addScanIterator(is);

        return new ScannerIterable<Vertex>(this, scan) {

          @Override
          public Vertex next(PeekingIterator<Entry<Key,Value>> iterator) {

            Entry<Key,Value> kv = iterator.next();
            AccumuloVertex v = null;
            if (vertexCache != null) {
              v = (AccumuloVertex) vertexCache.retrieve(kv.getKey().getRow().toString());
            }

            v = (v == null ? new AccumuloVertex(AccumuloGraph.this, kv.getKey().getRow().toString()) : v);
            v.cacheProperty(kv.getKey().getColumnFamily().toString(),
                AccumuloByteSerializer.deserialize(kv.getValue().get()));

            if (vertexCache != null) {
              vertexCache.cache(v);
            }
            return v;
          }
        };
      } else {
        // TODO
        throw new UnsupportedOperationException("Filtering on binary data not currently supported.");
      }
    }
  }

  @Override
  public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
    if (label == null) {
      throw ExceptionFactory.edgeLabelCanNotBeNull();
    }
    String myID;
    if (id == null) {
      myID = UUID.randomUUID().toString();
    } else {
      try {
        myID = id.toString();
      } catch (ClassCastException e) {
        return null;
      }
    }

    // TODO we arent suppose to make sure the given edge ID doesn't already
    // exist?

    try {
      Mutation m = new Mutation(myID);
      m.put(LABEL, (inVertex.getId().toString() + IDDELIM + outVertex.getId().toString()).getBytes(), AccumuloByteSerializer.serialize(label));
      edgeBW.addMutation(m);
      m = new Mutation(inVertex.getId().toString());
      m.put(INEDGE, (outVertex.getId().toString() + IDDELIM + myID).getBytes(), (IDDELIM + label).getBytes());
      vertexBW.addMutation(m);
      m = new Mutation(outVertex.getId().toString());
      m.put(OUTEDGE, (inVertex.getId().toString() + IDDELIM + myID).getBytes(), (IDDELIM + label).getBytes());
      vertexBW.addMutation(m);
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
      return null;
    }

    checkedFlush();

    AccumuloEdge edge = new AccumuloEdge(this, myID, label, inVertex, outVertex);
    if (edgeCache != null) {
      edgeCache.cache(edge);
    }
    return edge;
  }

  @Override
  public Edge getEdge(Object id) {
    if (id == null) {
      throw ExceptionFactory.edgeIdCanNotBeNull();
    }
    String myID = id.toString();

    Edge edge = null;
    if (edgeCache != null) {
      edge = edgeCache.retrieve(myID);
      if (edge != null) {
        return edge;
      }
    }
    Scanner s;
    if (!config.getSkipExistenceChecks()) {
      s = getElementScanner(Edge.class);
      s.setRange(new Range(myID, myID));
      s.fetchColumnFamily(TLABEL);

      if (config.getPreloadedProperties() != null) {
        for (String x : config.getPreloadedProperties()) {
          s.fetchColumnFamily(new Text(x));
        }
      }

      boolean found = s.iterator().hasNext();
      s.close();
      if (!found) {
        return null;
      }
    } else {
      return new AccumuloEdge(this, myID);
    }

    // Preload The Properties
    edge = new AccumuloEdge(this, myID);

    preloadProperties(s.iterator(), (AccumuloElement) edge);

    if (edgeCache != null) {
      edgeCache.cache(edge);
    }
    return edge;
  }

  private void preloadProperties(Iterator<Entry<Key,Value>> iter, AccumuloElement e) {
    while (iter.hasNext()) {
      Entry<Key,Value> entry = iter.next();
      Key key = entry.getKey();
      String attr = key.getColumnFamily().toString();
      if (SLABEL.equals(attr)) {
        if (!key.getColumnQualifier().toString().equals(SEXISTS)) {
          AccumuloEdge edge = (AccumuloEdge) e;
          String[] ids = key.getColumnQualifier().toString().split("_");
          edge.setInId(ids[0]);
          edge.setOutId(ids[1]);
          edge.setLabel(entry.getValue().toString());
        }
        continue;
      }
      Object val = AccumuloByteSerializer.deserialize(entry.getValue().get());
      e.cacheProperty(attr, val);
    }
  }

  @Override
  public void removeEdge(Edge edge) {
    if (!config.getIndexableGraphDisabled())
      clearIndex(edge.getId());

    if (edgeCache != null) {
      edgeCache.remove(edge.getId());
    }

    Scanner s = getElementScanner(Edge.class);
    s.setRange(new Range(edge.getId().toString()));

    Iterator<Entry<Key,Value>> iter = s.iterator();
    Text inVert = null;
    Text outVert = null;
    List<Mutation> indexMutations = new ArrayList<Mutation>();
    while (iter.hasNext()) {
      Entry<Key,Value> e = iter.next();
      Key k = e.getKey();
      if (k.getColumnFamily().equals(TLABEL)) {
        String[] ids = k.getColumnQualifier().toString().split(IDDELIM);
        inVert = new Text(ids[0]);
        outVert = new Text(ids[1]);
      } else {
        Mutation m = new Mutation(k.getColumnQualifier());
        m.putDelete(k.getColumnFamily(), k.getRow());
        indexMutations.add(m);
      }
    }
    s.close();
    if (inVert == null || outVert == null) {
      return;
    }

    BatchDeleter edgedeleter = null;
    try {
      getEdgeIndexWriter().addMutations(indexMutations);
      Mutation m = new Mutation(inVert);
      m.putDelete(INEDGE, (outVert.toString() + IDDELIM + edge.getId().toString()).getBytes());
      vertexBW.addMutation(m);
      m = new Mutation(outVert);
      m.putDelete(OUTEDGE, (inVert.toString() + IDDELIM + edge.getId().toString()).getBytes());
      vertexBW.addMutation(m);
      m = new Mutation(edge.getId().toString());
      m.putDelete(LABEL, (inVert.toString() + IDDELIM + outVert.toString()).getBytes());
      edgeBW.addMutation(m);

      checkedFlush();
      edgedeleter = config.getConnector().createBatchDeleter(config.getVertexTable(), config.getAuthorizations(), config.getQueryThreads(),
          config.getBatchWriterConfig());
      edgedeleter.setRanges(Collections.singleton(new Range(edge.getId().toString())));
      edgedeleter.delete();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (edgedeleter != null)
        edgedeleter.close();
    }
  }

  @Override
  public Iterable<Edge> getEdges() {
    BatchScanner scan = getElementBatchScanner(Edge.class);
    scan.fetchColumnFamily(TLABEL);

    if (config.getPreloadedProperties() != null) {
      for (String x : config.getPreloadedProperties()) {
        scan.fetchColumnFamily(new Text(x));
      }
    }

    return new ScannerIterable<Edge>(this, scan) {

      @Override
      public Edge next(PeekingIterator<Entry<Key,Value>> iterator) {
        // TODO I dont know if this should work with a batch scanner....
        Entry<Key,Value> entry = iterator.next();
        AccumuloEdge edge = new AccumuloEdge(AccumuloGraph.this, entry.getKey().getRow().toString(), AccumuloByteSerializer
            .deserialize(entry.getValue().get()).toString());

        String rowid = entry.getKey().getRow().toString();
        List<Entry<Key,Value>> vals = new ArrayList<Entry<Key,Value>>();
        while (iterator.peek() != null && rowid.compareToIgnoreCase(iterator.peek().getKey().getRow().toString()) == 0) {
          vals.add(iterator.next());
        }
        preloadProperties(vals.iterator(), edge);
        if (edgeCache != null) {
          edgeCache.cache(edge);
        }
        return edge;
      }
    };
  }

  @Override
  public Iterable<Edge> getEdges(String key, Object value) {
    nullCheckProperty(key, value);
    if (key.equalsIgnoreCase("label")) {
      key = SLABEL;
    }

    if (config.getAutoIndex() || getIndexedKeys(Edge.class).contains(key)) {
      // Use the index
      Scanner s = getEdgeIndexScanner();
      byte[] val = AccumuloByteSerializer.serialize(value);
      Text tVal = new Text(val);
      s.setRange(new Range(tVal, tVal));
      s.fetchColumnFamily(new Text(key));

      return new ScannerIterable<Edge>(this, s) {
        @Override
        public Edge next(PeekingIterator<Entry<Key,Value>> iterator) {
          AccumuloEdge e = null;
          Entry<Key,Value> kv = iterator.next();
          if (edgeCache != null) {
            e = (AccumuloEdge) edgeCache.retrieve(kv.getKey().getColumnQualifier().toString());
          }
          e = (e == null ? new AccumuloEdge(AccumuloGraph.this, kv.getKey().getColumnQualifier().toString()) : e);

          e.cacheProperty(kv.getKey().getColumnFamily().toString(),
                AccumuloByteSerializer.deserialize(kv.getKey().getRow().getBytes()));

          if (edgeCache != null) {
            edgeCache.cache(e);
          }
          return e;
        }
      };
    } else {

      BatchScanner scan = getElementBatchScanner(Edge.class);
      scan.fetchColumnFamily(new Text(key));

      byte[] val = AccumuloByteSerializer.serialize(value);
      if (val[0] != AccumuloByteSerializer.SERIALIZABLE) {
        IteratorSetting is = new IteratorSetting(10, "filter", RegExFilter.class);
        RegExFilter.setRegexs(is, null, null, null, Pattern.quote(new String(val)), false);
        scan.addScanIterator(is);

        return new ScannerIterable<Edge>(this, scan) {

          @Override
          public Edge next(PeekingIterator<Entry<Key,Value>> iterator) {

            Key k = iterator.next().getKey();

            if (k.getColumnFamily().compareTo(AccumuloGraph.TLABEL) == 0) {
              String[] vals = k.getColumnQualifier().toString().split(AccumuloGraph.IDDELIM);
              return new AccumuloEdge(AccumuloGraph.this, k.getRow().toString(), null, vals[0], vals[1]);
            }
            return new AccumuloEdge(AccumuloGraph.this, k.getRow().toString());
          }
        };
      } else {
        // TODO
        throw new UnsupportedOperationException("Filtering on binary data not currently supported.");
      }
    }
  }

  // TODO Eventually
  @Override
  public GraphQuery query() {
    return new DefaultGraphQuery(this);
  }

  @Override
  public void shutdown() {
    try {
      writer.close();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }
    if (vertexCache != null) {
      vertexCache.clear();
    }

    if (edgeCache != null) {
      edgeCache.clear();
    }
  }

  // public methods not defined by Graph interface, but potentially useful for
  // applications that know they are using an AccumuloGraph
  public void clear() {
    shutdown();

    try {
      TableOperations to;
      to = config.getConnector().tableOperations();
      Iterable<Index<? extends Element>> it = this.getIndices();
      Iterator<Index<? extends Element>> iter = it.iterator();
      while (iter.hasNext()) {
        AccumuloIndex<?> in = (AccumuloIndex<?>) iter.next();
        to.delete(in.tableName);
      }

      for (String t : config.getTableNames()) {
        if (to.exists(t)) {
          to.delete(t);
          to.create(t);
          SortedSet<Text> splits = config.getSplits();
          if (splits != null) {
            to.addSplits(t, splits);
          }
        }
      }
      setupWriters();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    }

  }

  public void flush() {
    try {
      writer.flush();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }
  }

  public void clearCache() {
    vertexCache.clear();
    edgeCache.clear();
  }

  private void checkedFlush() {
    if (config.getAutoFlush()) {
      flush();
    }
  }

  // methods used by AccumuloElement, AccumuloVertex, AccumuloEdge to interact
  // with the backing Accumulo data store...

  <T> Pair<Integer,T> getProperty(Class<? extends Element> type, String id, String key) {
    Text colf = null;
    if (StringFactory.LABEL.equals(key)) {
      colf = AccumuloGraph.TLABEL;
    } else {
      colf = new Text(key);
    }

    Scanner s = getElementScanner(type);
    s.setRange(new Range(id));
    s.fetchColumnFamily(colf);
    T toRet = null;
    Iterator<Entry<Key,Value>> iter = s.iterator();
    if (iter.hasNext()) {
      toRet = AccumuloByteSerializer.deserialize(iter.next().getValue().get());
    }
    s.close();
    return new Pair<Integer,T>(config.getPropertyCacheTimeout(key), toRet);
  }

  void preloadProperties(AccumuloElement element, Class<? extends Element> type) {
    String[] toPreload = config.getPreloadedProperties();
    if (toPreload == null) {
      return;
    }

    Scanner s = getElementScanner(type);
    s.setRange(new Range(element.getId().toString()));

    // user has requested specific properties...
    Text colf = new Text("");
    for (String key : toPreload) {
      if (StringFactory.LABEL.equals(key)) {
        colf.set(AccumuloGraph.LABEL);
      } else {
        colf.set(key);
      }
      s.fetchColumnFamily(colf);
    }

    Iterator<Entry<Key,Value>> iter = s.iterator();
    // Integer timeout = config.getPropertyCacheTimeoutMillis(); // Change this
    while (iter.hasNext()) {
      Entry<Key,Value> entry = iter.next();
      Object val = AccumuloByteSerializer.deserialize(entry.getValue().get());
      element.cacheProperty(entry.getKey().getColumnFamily().toString(), val);
    }
    s.close();
  }

  Set<String> getPropertyKeys(Class<? extends Element> type, String id) {
    Scanner s = getElementScanner(type);
    s.setRange(new Range(id));
    Set<String> toRet = new HashSet<String>();
    Iterator<Entry<Key,Value>> iter = s.iterator();
    while (iter.hasNext()) {
      Entry<Key,Value> e = iter.next();
      Key k = e.getKey();
      String cf = k.getColumnFamily().toString();
      toRet.add(cf);
    }
    toRet.remove(TINEDGE.toString());
    toRet.remove(TLABEL.toString());
    toRet.remove(TOUTEDGE.toString());
    s.close();
    return toRet;
  }

  /**
   * Sets the property. Requires a round-trip to Accumulo to see if the property exists
   * iff the provided key has an index. Therefore, for best performance, if at
   * all possible, create indices after bulk ingest.
   * 
   * @param type
   * @param id
   * @param key
   * @param val
   */
  void setProperty(Class<? extends Element> type, String id, String key, Object val) {
    checkProperty(key, val);
    try {
      byte[] newByteVal = AccumuloByteSerializer.serialize(val);
      Mutation m = null;

      if (config.getAutoIndex() || getIndexedKeys(type).contains(key)) {
        BatchWriter bw = getIndexBatchWriter(type);
        Object old = getProperty(type, id, key).getSecond();
        if (old != null) {
          byte[] oldByteVal = AccumuloByteSerializer.serialize(old);
          m = new Mutation(oldByteVal);
          m.putDelete(key, id);
          bw.addMutation(m);
        }
        m = new Mutation(newByteVal);
        m.put(key.getBytes(), id.getBytes(), EMPTY);
        bw.addMutation(m);
        checkedFlush();
      }

      m = new Mutation(id);
      m.put(key.getBytes(), EMPTY, newByteVal);
      getBatchWriter(type).addMutation(m);

      checkedFlush();
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }
  }

  private BatchWriter getBatchWriter(Class<? extends Element> type) {
    if (type.equals(Edge.class))
      return edgeBW;
    return vertexBW;
  }

  private BatchWriter getIndexBatchWriter(Class<? extends Element> type) {
    if (type.equals(Edge.class))
      return getEdgeIndexWriter();
    return getVertexIndexWriter();
  }

  <T> T removeProperty(Class<? extends Element> type, String id, String key) {
    if (StringFactory.LABEL.equals(key) || SLABEL.equals(key)) {
      throw new AccumuloGraphException("Cannot remove the " + StringFactory.LABEL + " property.");
    }

    T obj = (T) getProperty(type, id, key).getSecond();
    try {
      if (obj != null) {
        byte[] val = AccumuloByteSerializer.serialize(obj);
        Mutation m = new Mutation(id);
        m.putDelete(key.getBytes(), EMPTY);
        BatchWriter bw = getBatchWriter(type);
        bw.addMutation(m);
        m = new Mutation(val);
        m.putDelete(key, id);
        getIndexBatchWriter(type).addMutation(m);
        checkedFlush();
      }
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }
    return obj;
  }

  Iterable<Edge> getEdges(String vertexId, Direction direction, String... labels) {
    Scanner scan = getElementScanner(Vertex.class);
    scan.setRange(new Range(vertexId));
    if (direction.equals(Direction.IN)) {
      scan.fetchColumnFamily(TINEDGE);
    } else if (direction.equals(Direction.OUT)) {
      scan.fetchColumnFamily(TOUTEDGE);
    } else {
      scan.fetchColumnFamily(TINEDGE);
      scan.fetchColumnFamily(TOUTEDGE);
    }
    if (labels.length > 0) {
      applyRegexValueFilter(scan, labels);
    }

    return new ScannerIterable<Edge>(this, scan) {

      @Override
      public Edge next(PeekingIterator<Entry<Key,Value>> iterator) {
        // TODO better use of information readily available...
        // TODO could also check local cache before creating a new
        // instance?

        Entry<Key,Value> kv = iterator.next();

        String[] parts = kv.getKey().getColumnQualifier().toString().split(IDDELIM);
        String label = (new String(kv.getValue().get())).split("_")[1];
        if (kv.getKey().getColumnFamily().toString().equalsIgnoreCase(AccumuloGraph.SINEDGE)) {
          return new AccumuloEdge(AccumuloGraph.this, parts[1], label, kv.getKey().getRow().toString(), parts[0]);

        } else {
          return new AccumuloEdge(AccumuloGraph.this, parts[1], label, parts[0], kv.getKey().getRow().toString());

        }
      }
    };
  }

  private void applyRegexValueFilter(Scanner scan, String... labels) {
    StringBuilder regex = new StringBuilder();
    for (String lab : labels) {
      if (regex.length() != 0)
        regex.append("|");
      regex.append(".*_\\Q").append(lab).append("\\E$");
    }

    IteratorSetting is = new IteratorSetting(10, "getEdgeFilter", RegExFilter.class);
    RegExFilter.setRegexs(is, null, null, null, regex.toString(), false);
    scan.addScanIterator(is);
  }

  Iterable<Vertex> getVertices(String vertexId, Direction direction, String... labels) {
    Scanner scan = getElementScanner(Vertex.class);
    scan.setRange(new Range(vertexId));
    if (direction.equals(Direction.IN)) {
      scan.fetchColumnFamily(TINEDGE);
    } else if (direction.equals(Direction.OUT)) {
      scan.fetchColumnFamily(TOUTEDGE);
    } else {
      scan.fetchColumnFamily(TINEDGE);
      scan.fetchColumnFamily(TOUTEDGE);
    }

    if (labels != null && labels.length > 0) {
      applyRegexValueFilter(scan, labels);
    }

    return new ScannerIterable<Vertex>(this, scan) {

      @Override
      public Vertex next(PeekingIterator<Entry<Key,Value>> iterator) {
        // TODO better use of information readily available...
        // TODO could also check local cache before creating a new
        // instance?
        String[] parts = iterator.next().getKey().getColumnQualifier().toString().split(IDDELIM);
        AccumuloVertex v = new AccumuloVertex(AccumuloGraph.this, parts[0]);
        if (vertexCache != null)
          vertexCache.cache(v);
        return v;
      }
    };
  }

  Vertex getEdgeVertex(String edgeId, Direction direction) {
    Scanner s = getElementScanner(Edge.class);
    try {
      s.setRange(new Range(edgeId));
      s.fetchColumnFamily(TLABEL);
      Iterator<Entry<Key,Value>> iter = s.iterator();
      if (!iter.hasNext()) {
        return null;
      }
      String id;
      String val = iter.next().getKey().getColumnQualifier().toString();
      if (direction == Direction.IN) {
        id = val.split(IDDELIM)[0];
      } else {
        id = val.split(IDDELIM)[1];
      }
      Vertex v = new AccumuloVertex(this, id);
      if (vertexCache != null)
        vertexCache.cache(v);
      return v;

    } finally {
      s.close();
    }
  }

  private void nullCheckProperty(String key, Object val) {
    if (key == null) {
      throw ExceptionFactory.propertyKeyCanNotBeNull();
    } else if (val == null) {
      throw ExceptionFactory.propertyValueCanNotBeNull();
    } else if (key.trim().equals(StringFactory.EMPTY_STRING)) {
      throw ExceptionFactory.propertyKeyCanNotBeEmpty();
    }
  }

  // internal methods used by this class

  private void checkProperty(String key, Object val) {
    nullCheckProperty(key, val);
    if (key.equals(StringFactory.ID)) {
      throw ExceptionFactory.propertyKeyIdIsReserved();
    } else if (key.equals(StringFactory.LABEL)) {
      throw ExceptionFactory.propertyKeyLabelIsReservedForEdges();
    } else if (val == null) {
      throw ExceptionFactory.propertyValueCanNotBeNull();
    }
  }

  @Override
  public String toString() {
    return "accumulograph";
  }

  @Override
  public <T extends Element> Index<T> createIndex(String indexName,
      Class<T> indexClass, Parameter... indexParameters) {
    if (indexClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }
    if (config.getIndexableGraphDisabled())
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");

    Scanner s = this.getMetadataScanner();
    try {
      s.setRange(new Range(indexName, indexName));
      if (s.iterator().hasNext())
        throw ExceptionFactory.indexAlreadyExists(indexName);

      BatchWriter writer = getWriter(config.getMetadataTable());
      Mutation m = new Mutation(indexName);
      m.put(indexClass.getSimpleName().getBytes(), EMPTY, EMPTY);
      try {
        writer.addMutation(m);
      } catch (MutationsRejectedException e) {
        e.printStackTrace();
      }
      return new AccumuloIndex<T>(indexClass, this, indexName);
    } finally {
      s.close();
    }
  }

  @Override
  public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
    if (indexClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }
    if (config.getIndexableGraphDisabled())
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");

    Scanner scan = getScanner(config.getMetadataTable());
    try {
      scan.setRange(new Range(indexName, indexName));
      Iterator<Entry<Key,Value>> iter = scan.iterator();

      while (iter.hasNext()) {
        Key k = iter.next().getKey();
        if (k.getColumnFamily().toString().equals(indexClass.getSimpleName())) {
          return new AccumuloIndex<T>(indexClass, this, indexName);
        } else {
          throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
        }
      }
      return null;
    } finally {
      scan.close();
    }
  }

  @Override
  public Iterable<Index<? extends Element>> getIndices() {
    if (config.getIndexableGraphDisabled())
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");
    List<Index<? extends Element>> toRet = new ArrayList<Index<? extends Element>>();
    Scanner scan = getScanner(config.getMetadataTable());
    try {
      Iterator<Entry<Key,Value>> iter = scan.iterator();

      while (iter.hasNext()) {
        Key k = iter.next().getKey();
        toRet.add(new AccumuloIndex(getClass(k.getColumnFamily().toString()),
            this, k.getRow().toString()));
      }
      return toRet;
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      scan.close();
    }
  }

  private Class<? extends Element> getClass(String e) {
    if (e.equals("Vertex")) {
      return Vertex.class;
    }
    return Edge.class;
  }

  @Override
  public void dropIndex(String indexName) {
    if (config.getIndexableGraphDisabled())
      throw new UnsupportedOperationException("IndexableGraph is disabled via the configuration");
    BatchDeleter deleter = null;
    try {

      deleter = config.getConnector().createBatchDeleter(config.getMetadataTable(), config.getAuthorizations(), config.getQueryThreads(),
          config.getBatchWriterConfig());
      deleter.setRanges(Collections.singleton(new Range(indexName)));
      deleter.delete();
      config.getConnector().tableOperations().delete(config.getGraphName() + "_index_" + indexName);
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (deleter != null)
        deleter.close();
    }
  }

  @Override
  public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }

    String table = null;
    if (elementClass.equals(Vertex.class)) {
      table = config.getVertexIndexTable();
    } else {
      table = config.getEdgeIndexTable();
    }
    BatchWriter w = getKeyMetadataWriter();
    BatchDeleter bd = null;
    Mutation m = new Mutation(key);
    m.putDelete(elementClass.getSimpleName().getBytes(), EMPTY);
    try {
      bd = config.getConnector().createBatchDeleter(table, config.getAuthorizations(), config.getMaxWriteThreads(), config.getBatchWriterConfig());
      w.addMutation(m);
      bd.setRanges(Collections.singleton(new Range()));
      bd.fetchColumnFamily(new Text(key));
      bd.delete();
    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (bd != null)
        bd.close();
    }
    checkedFlush();
  }

  @Override
  public <T extends Element> void createKeyIndex(String key,
      Class<T> elementClass, Parameter... indexParameters) {
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }
    BatchWriter w = getKeyMetadataWriter();

    Mutation m = new Mutation(key);
    m.put(elementClass.getSimpleName().getBytes(), EMPTY, EMPTY);
    try {
      w.addMutation(m);
    } catch (MutationsRejectedException e) {
      e.printStackTrace();
    }
    checkedFlush();
    // Re Index Graph
    BatchScanner scan = getElementBatchScanner(elementClass);
    try {
      scan.setRanges(Collections.singleton(new Range()));
      scan.fetchColumnFamily(new Text(key));
      Iterator<Entry<Key,Value>> iter = scan.iterator();

      BatchWriter bw = getIndexBatchWriter(elementClass);
      while (iter.hasNext()) {
        Entry<Key,Value> entry = iter.next();
        Key k = entry.getKey();
        Value v = entry.getValue();
        Mutation mu = new Mutation(v.get());
        mu.put(k.getColumnFamily().getBytes(), k.getRow().getBytes(), EMPTY);
        try {
          bw.addMutation(mu);
        } catch (MutationsRejectedException e) {
          // TODO handle this better.
          throw new AccumuloGraphException(e);
        }
      }
    } finally {
      scan.close();
    }
    checkedFlush();

  }

  @Override
  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    if (elementClass == null) {
      throw ExceptionFactory.classForElementCannotBeNull();
    }

    Scanner s = getKeyMetadataScanner();

    try {
      s.fetchColumnFamily(new Text(elementClass.getSimpleName()));
      Iterator<Entry<Key,Value>> iter = s.iterator();
      Set<String> toRet = new HashSet<String>();
      while (iter.hasNext()) {
        toRet.add(iter.next().getKey().getRow().toString());
      }
      return toRet;
    } finally {
      s.close();
    }
  }

  public boolean isEmpty() {
    for (String t : config.getTableNames()) {
      if (getScanner(t).iterator().hasNext()) {
        return false;
      }
    }

    return true;
  }
}
