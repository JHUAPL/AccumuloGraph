/******************************************************************************
 *                              COPYRIGHT NOTICE                              *
 * Copyright (c) 2014 The Johns Hopkins University/Applied Physics Laboratory *
 *                            All rights reserved.                            *
 *                                                                            *
 * This material may only be used, modified, or reproduced by or for the      *
 * U.S. Government pursuant to the license rights granted under FAR clause    *
 * 52.227-14 or DFARS clauses 252.227-7013/7014.                              *
 *                                                                            *
 * For any other permissions, please contact the Legal Office at JHU/APL.     *
 ******************************************************************************/
package edu.jhuapl.tinkerpop.tables;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloEdge;
import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.ScannerIterable;
import edu.jhuapl.tinkerpop.mutator.vertex.AddVertexMutator;
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
    Mutators.apply(getWriter(), new AddVertexMutator(vertex));
    globals.checkedFlush();
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

  public Iterable<Edge> getEdges(Vertex vertex, Direction direction,
      String... labels) {
    Scanner scan = getScanner();
    scan.setRange(new Range(vertex.getId().toString()));
    if (direction.equals(Direction.IN)) {
      scan.fetchColumnFamily(AccumuloGraph.TINEDGE);
    } else if (direction.equals(Direction.OUT)) {
      scan.fetchColumnFamily(AccumuloGraph.TOUTEDGE);
    } else {
      scan.fetchColumnFamily(AccumuloGraph.TINEDGE);
      scan.fetchColumnFamily(AccumuloGraph.TOUTEDGE);
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

        String[] parts = kv.getKey().getColumnQualifier().toString().split(AccumuloGraph.IDDELIM);
        String label = (new String(kv.getValue().get())).split("_")[1];

        AccumuloEdge edge;
        if (kv.getKey().getColumnFamily().toString().equalsIgnoreCase(AccumuloGraph.SINEDGE)) {
          edge = new AccumuloEdge(globals, parts[1], label,
              kv.getKey().getRow().toString(), parts[0]);
        } else {
          edge = new AccumuloEdge(globals, parts[1], label,
              parts[0], kv.getKey().getRow().toString());
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
      scan.fetchColumnFamily(AccumuloGraph.TINEDGE);
    } else if (direction.equals(Direction.OUT)) {
      scan.fetchColumnFamily(AccumuloGraph.TOUTEDGE);
    } else {
      scan.fetchColumnFamily(AccumuloGraph.TINEDGE);
      scan.fetchColumnFamily(AccumuloGraph.TOUTEDGE);
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
            .toString().split(AccumuloGraph.IDDELIM);

        AccumuloVertex vertex = new AccumuloVertex(globals, parts[0]);
        globals.getCaches().cache(vertex, Vertex.class);

        return vertex;
      }
    };
  }

  public Iterable<Vertex> getVertices() {
    Scanner scan = getScanner();
    scan.fetchColumnFamily(AccumuloGraph.TLABEL);

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

        return parser.parse(rowId, entries);
      }
    };
  }
}
