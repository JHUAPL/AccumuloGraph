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
package edu.jhuapl.tinkerpop.tables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloEdge;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloGraphUtils;
import edu.jhuapl.tinkerpop.AccumuloVertex;
import edu.jhuapl.tinkerpop.Constants;
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
    scan.fetchColumnFamily(new Text(Constants.LABEL));

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

  public Iterable<Edge> getEdges(String key, Object value) {
    AccumuloGraphUtils.nullCheckProperty(key, value);
    if (key.equalsIgnoreCase("label")) {
      key = Constants.LABEL;
    }
    
    BatchScanner scan = getBatchScanner();
    scan.fetchColumnFamily(new Text(key));

    byte[] val = AccumuloByteSerializer.serialize(value);
    if (val[0] != AccumuloByteSerializer.SERIALIZABLE) {
      IteratorSetting is = new IteratorSetting(10, "filter", RegExFilter.class);
      RegExFilter.setRegexs(is, null, null, null, Pattern.quote(new String(val)), false);
      scan.addScanIterator(is);

      return new ScannerIterable<Edge>(scan) {

        @Override
        public Edge next(PeekingIterator<Entry<Key,Value>> iterator) {

          Key k = iterator.next().getKey();

          if (k.getColumnFamily().toString().equals(Constants.LABEL)) {
            String[] vals = k.getColumnQualifier().toString().split(Constants.ID_DELIM);
            return new AccumuloEdge(globals, k.getRow().toString(),
                new AccumuloVertex(globals, vals[0]),
                new AccumuloVertex(globals, vals[1]), null);
          }
          return new AccumuloEdge(globals, k.getRow().toString());
        }
      };
    } else {
      // TODO
      throw new UnsupportedOperationException("Filtering on binary data not currently supported.");
    }
  }

  public void loadEndpointsAndLabel(AccumuloEdge edge) {
    Scanner s = getScanner();

    try {
      s.setRange(new Range(edge.getId().toString()));
      s.fetchColumnFamily(new Text(Constants.LABEL));
      Iterator<Entry<Key,Value>> iter = s.iterator();
      if (!iter.hasNext()) {
        dump();
        throw new AccumuloGraphException("Unable to find edge row: "+edge);
      }

      Entry<Key, Value> entry = iter.next();

      String cq = entry.getKey().getColumnQualifier().toString();
      String[] ids = cq.split(Constants.ID_DELIM);

      String label = AccumuloByteSerializer.deserialize(entry.getValue().get());

      edge.setVertices(new AccumuloVertex(globals, ids[0]),
          new AccumuloVertex(globals, ids[1]));
      edge.setLabel(label);

    } finally {
      s.close();
    }
  }
}
