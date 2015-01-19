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
package edu.jhuapl.tinkerpop.mutator.edge;

import org.apache.accumulo.core.data.Mutation;

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.Constants;

public class EdgeEndpointsMutator {

  private EdgeEndpointsMutator() {
  }

  public static class Add extends BaseEdgeMutator {

    public Add(Edge edge) {
      super(edge);
    }

    @Override
    public Iterable<Mutation> create() {
      String inVertexId = edge.getVertex(Direction.IN).getId().toString();
      String outVertexId = edge.getVertex(Direction.OUT).getId().toString();

      Mutation in = new Mutation(inVertexId);
      in.put(Constants.IN_EDGE.getBytes(),
          (outVertexId + Constants.ID_DELIM + edge.getId()).getBytes(),
          (Constants.ID_DELIM + edge.getLabel()).getBytes());

      Mutation out = new Mutation(outVertexId);
      out.put(Constants.OUT_EDGE.getBytes(),
          (inVertexId + Constants.ID_DELIM + edge.getId()).getBytes(),
          (Constants.ID_DELIM + edge.getLabel()).getBytes());

      return Lists.newArrayList(in, out);
    }
  }

  public static class Delete extends BaseEdgeMutator {

    public Delete(Edge edge) {
      super(edge);
    }

    @Override
    public Iterable<Mutation> create() {
      String inVertexId = edge.getVertex(Direction.IN).getId().toString();
      String outVertexId = edge.getVertex(Direction.OUT).getId().toString();

      Mutation in = new Mutation(inVertexId);
      in.putDelete(Constants.IN_EDGE.getBytes(),
          (outVertexId + Constants.ID_DELIM + edge.getId()).getBytes());

      Mutation out = new Mutation(outVertexId);
      out.putDelete(Constants.OUT_EDGE.getBytes(),
          (inVertexId + Constants.ID_DELIM + edge.getId()).getBytes());

      return Lists.newArrayList(in, out);
    }
  }
}
