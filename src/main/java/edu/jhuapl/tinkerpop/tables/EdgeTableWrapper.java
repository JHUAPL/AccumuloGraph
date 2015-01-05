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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloEdge;
import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.GlobalInstances;
import edu.jhuapl.tinkerpop.ScannerIterable;
import edu.jhuapl.tinkerpop.mutator.Mutators;
import edu.jhuapl.tinkerpop.mutator.edge.EdgeMutator;
import edu.jhuapl.tinkerpop.parser.EdgeParser;


/**
 * Wrapper around {@link Edge} tables.
 */
public class EdgeTableWrapper extends ElementTableWrapper {

  public EdgeTableWrapper(GlobalInstances globals) {
    super(globals, globals.getConfig().getEdgeTableName());
  }

  /**
   * Write the given edge to the edge table. Does not
   * currently write the edge's properties.
   * 
   * <p/>Note: This only adds the edge information. Vertex
   * endpoint information needs to be written to the vertex
   * table via {@link VertexTableWrapper}.
   * @param edge
   */
  public void writeEdge(Edge edge) {
    Mutators.apply(getWriter(), new EdgeMutator.Add(edge));
    globals.checkedFlush();
  }

  public void deleteEdge(Edge edge) {
    Mutators.apply(getWriter(), new EdgeMutator.Delete(edge));
    globals.checkedFlush();
  }

  public Iterable<Edge> getEdges() {
    Scanner scan = getScanner();
    scan.fetchColumnFamily(AccumuloGraph.TLABEL);

    if (globals.getConfig().getPreloadedProperties() != null) {
      for (String key : globals.getConfig().getPreloadedProperties()) {
        scan.fetchColumnFamily(new Text(key));
      }
    }

    final EdgeParser parser = new EdgeParser(globals);

    return new ScannerIterable<Edge>(scan) {
      @Override
      public Edge next(PeekingIterator<Entry<Key, Value>> iterator) {
        // TODO could also check local cache before creating a new instance?

        String rowId = iterator.peek().getKey().getRow().toString();

        List<Entry<Key, Value>> entries =
            new ArrayList<Entry<Key, Value>>();

        // MDL 05 Jan 2014:  Why is this equalsIgnoreCase??
        while (iterator.peek() != null && rowId.equalsIgnoreCase(iterator
            .peek().getKey().getRow().toString())) {
          entries.add(iterator.next());
        }

        AccumuloEdge edge = parser.parse(rowId, entries);
        globals.getCaches().cache(edge, Edge.class);

        return edge;
      }
    };
  }

  public void loadEndpointsAndLabel(AccumuloEdge edge) {
    Scanner s = getScanner();

    try {
      s.setRange(new Range(edge.getId().toString()));
      s.fetchColumnFamily(AccumuloGraph.TLABEL);
      Iterator<Entry<Key,Value>> iter = s.iterator();
      if (!iter.hasNext()) {
        dump();
        throw new AccumuloGraphException("Unable to find edge row: "+edge);
      }

      Entry<Key, Value> entry = iter.next();

      String cq = entry.getKey().getColumnQualifier().toString();
      String[] ids = cq.split(AccumuloGraph.IDDELIM);

      String label = AccumuloByteSerializer.deserialize(entry.getValue().get());

      edge.setVertices(new AccumuloVertex(globals, ids[0]),
          new AccumuloVertex(globals, ids[1]));
      edge.setLabel(label);

    } finally {
      s.close();
    }
  }
}
