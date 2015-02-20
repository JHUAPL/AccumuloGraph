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
package edu.jhuapl.tinkerpop.tables.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloEdge;
import edu.jhuapl.tinkerpop.AccumuloElement;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloGraphUtils;
import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.Constants;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.ScannerIterable;
import edu.jhuapl.tinkerpop.mutator.vertex.AddVertexMutator;
import edu.jhuapl.tinkerpop.mutator.Mutator;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.edge.EdgeEndpointsMutator;
import edu.jhuapl.tinkerpop.parser.VertexParser;


/**
 * Wrapper around {@link Vertex} tables.
 */
public class VertexTableWrapper extends ElementTableWrapper {

  public VertexTableWrapper(GlobalInstances globals) {
    super(globals, globals.getConfig().getVertexTableName());
  }

  /**
   * Write a vertex with the given id.
   * Note: This does not currently write the vertex's properties.
   * @param vertex
   */
  public void writeVertex(Vertex vertex) {
    Mutators.apply(getWriter(), new AddVertexMutator(vertex.getId().toString()));
    globals.checkedFlush();
  }

  /**
   * Remove the given vertex.
   * Note: This uses a BatchDeleter rather than {@link Mutator}
   * because it is more efficient.
   * @param vertex
   */
  public void deleteVertex(Vertex vertex) {
    BatchDeleter deleter = null;

    try {
      deleter = getDeleter();
      deleter.setRanges(Arrays.asList(Range.exact((String) vertex.getId())));
      deleter.delete();

    } catch (Exception e) {
      throw new AccumuloGraphException(e);
    } finally {
      if (deleter != null) {
        deleter.close();
      }
    }
  }

  /**
   * Write edge endpoint information to the vertex table.
   * @param edge
   */
  public void writeEdgeEndpoints(Edge edge) {
    Mutators.apply(getWriter(), new EdgeEndpointsMutator.Add(edge));
    globals.checkedFlush();
  }

  public void deleteEdgeEndpoints(Edge edge) {
    Mutators.apply(getWriter(), new EdgeEndpointsMutator.Delete(edge));
    globals.checkedFlush();
  }

  public CloseableIterable<Edge> getEdges(Vertex vertex, Direction direction,
      String... labels) {
    Scanner scan = getScanner();
    scan.setRange(new Range(vertex.getId().toString()));
    if (direction.equals(Direction.IN)) {
      scan.fetchColumnFamily(new Text(Constants.IN_EDGE));
    } else if (direction.equals(Direction.OUT)) {
      scan.fetchColumnFamily(new Text(Constants.OUT_EDGE));
    } else {
      scan.fetchColumnFamily(new Text(Constants.IN_EDGE));
      scan.fetchColumnFamily(new Text(Constants.OUT_EDGE));
    }

    if (labels.length > 0) {
      applyEdgeLabelValueFilter(scan, labels);
    }

    return new ScannerIterable<Edge>(scan) {

      @Override
      public Edge next(PeekingIterator<Entry<Key,Value>> iterator) {
        // TODO better use of information readily available...
        // TODO could also check local cache before creating a new
        // instance?

        Entry<Key,Value> kv = iterator.next();

        String[] parts = kv.getKey().getColumnQualifier().toString().split(Constants.ID_DELIM);
        String label = (new String(kv.getValue().get())).split(Constants.ID_DELIM)[1];

        AccumuloEdge edge;
        if (kv.getKey().getColumnFamily().toString().equalsIgnoreCase(Constants.IN_EDGE)) {
          edge = new AccumuloEdge(globals, parts[1],
              new AccumuloVertex(globals, kv.getKey().getRow().toString()),
              new AccumuloVertex(globals, parts[0]), label);
        } else {
          edge = new AccumuloEdge(globals, parts[1],
              new AccumuloVertex(globals, parts[0]),
              new AccumuloVertex(globals, kv.getKey().getRow().toString()), label);
        }
        globals.getCaches().cache(edge, Edge.class);

        return edge;
      }
    };
  }

  public Iterable<Vertex> getVertices(Vertex vertex, Direction direction, String... labels) {
    Scanner scan = getScanner();
    scan.setRange(new Range(vertex.getId().toString()));
    if (direction.equals(Direction.IN)) {
      scan.fetchColumnFamily(new Text(Constants.IN_EDGE));
    } else if (direction.equals(Direction.OUT)) {
      scan.fetchColumnFamily(new Text(Constants.OUT_EDGE));
    } else {
      scan.fetchColumnFamily(new Text(Constants.IN_EDGE));
      scan.fetchColumnFamily(new Text(Constants.OUT_EDGE));
    }

    if (labels != null && labels.length > 0) {
      applyEdgeLabelValueFilter(scan, labels);
    }

    return new ScannerIterable<Vertex>(scan) {

      @Override
      public Vertex next(PeekingIterator<Entry<Key,Value>> iterator) {
        // TODO better use of information readily available...
        // TODO could also check local cache before creating a new
        // instance?
        String[] parts = iterator.next().getKey().getColumnQualifier()
            .toString().split(Constants.ID_DELIM);

        AccumuloVertex vertex = new AccumuloVertex(globals, parts[0]);
        globals.getCaches().cache(vertex, Vertex.class);

        return vertex;
      }
    };
  }

  public CloseableIterable<Vertex> getVertices() {
    return getVerticesInRange(null, null);
  }

  public CloseableIterable<Vertex> getVerticesInRange(Object fromId, Object toId) {
    Scanner scan = getScanner();
    scan.setRange(new Range(fromId != null ? fromId.toString() : null,
        toId != null ? toId.toString() : null));
    scan.fetchColumnFamily(new Text(Constants.LABEL));

    if (globals.getConfig().getPreloadedProperties() != null) {
      for (String key : globals.getConfig().getPreloadedProperties()) {
        scan.fetchColumnFamily(new Text(key));
      }
    }

    final VertexParser parser = new VertexParser(globals);

    return new ScannerIterable<Vertex>(scan) {
      @Override
      public Vertex next(PeekingIterator<Entry<Key, Value>> iterator) {
        // TODO could also check local cache before creating a new instance?

        String rowId = iterator.peek().getKey().getRow().toString();

        List<Entry<Key, Value>> entries =
            new ArrayList<Entry<Key, Value>>();

        while (iterator.peek() != null && rowId.equals(iterator
            .peek().getKey().getRow().toString())) {
          entries.add(iterator.next());
        }

        AccumuloVertex vertex = parser.parse(rowId, entries);
        globals.getCaches().cache(vertex, Vertex.class);

        return vertex;
      }
    };
  }

  public Iterable<Vertex> getVertices(String key, Object value) {
    AccumuloGraphUtils.validateProperty(key, value);

    byte[] val = AccumuloByteSerializer.serialize(value);
    if (val[0] != AccumuloByteSerializer.SERIALIZABLE) {
      BatchScanner scan = getBatchScanner();
      scan.fetchColumnFamily(new Text(key));

      IteratorSetting is = new IteratorSetting(10, "filter", RegExFilter.class);
      RegExFilter.setRegexs(is, null, null, null, Pattern.quote(new String(val)), false);
      scan.addScanIterator(is);

      return new ScannerIterable<Vertex>(scan) {

        @Override
        public Vertex next(PeekingIterator<Entry<Key,Value>> iterator) {
          Entry<Key, Value> kv = iterator.next();
          String key = kv.getKey().getColumnFamily().toString();
          Object value = AccumuloByteSerializer.deserialize(kv.getValue().get());

          Vertex v = globals.getCaches().retrieve(kv.getKey().getRow().toString(), Vertex.class);
          if (v == null) {
            v = new AccumuloVertex(globals, kv.getKey().getRow().toString());
          }

          ((AccumuloElement) v).setPropertyInMemory(key, value);
          globals.getCaches().cache(v, Vertex.class);

          return v;
        }
      };
    } else {
      // TODO
      throw new UnsupportedOperationException("Filtering on binary data not currently supported.");
    }
  }
}
