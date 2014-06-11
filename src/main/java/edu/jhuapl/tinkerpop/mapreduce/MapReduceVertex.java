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
package edu.jhuapl.tinkerpop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

import edu.jhuapl.tinkerpop.AccumuloGraph;

public class MapReduceVertex extends MapReduceElement implements Vertex {

	List<Edge> inEdges;
	List<Edge> outEdges;

	MapReduceVertex(AccumuloGraph parent) {
		super(parent);
		inEdges = new LinkedList<Edge>();
		outEdges = new LinkedList<Edge>();
	}

	void prepareEdge(String id, String src, String label, String dest) {
		MapReduceEdge mre = new MapReduceEdge(parent, id, src, label, dest);
		if (src.equals(getId())) {
			outEdges.add(mre);
		}
		// maybe this could be an "else if"-- do we support self-referential
		// edges??
		if (dest.equals(getId())) {
			inEdges.add(mre);
		}
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex) {
		throw new UnsupportedOperationException(
				"You cannot modify a vertex during a MapReduce job.");
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		switch (direction) {
		case IN:
			return getEdges(inEdges, labels);
		case OUT:
			return getEdges(outEdges, labels);
		case BOTH:
			Set<Edge> all = new HashSet<Edge>(inEdges.size() + outEdges.size());
			all.addAll(inEdges);
			all.addAll(outEdges);
			return getEdges(all, labels);
		default:
			throw new RuntimeException("Unexpected direction: " + direction);
		}
	}

	private Iterable<Edge> getEdges(Collection<Edge> edges, String... labels) {
		if (labels.length > 0) {
			List<String> filters = Arrays.asList(labels);
			LinkedList<Edge> filteredEdges = new LinkedList<Edge>();
			for (Edge e : edges) {
				if (filters.contains(e.getLabel())) {
					filteredEdges.add(e);
				}
			}
			return filteredEdges;
		} else {
			return edges;
		}
	}

	@Override
	public Iterable<Vertex> getVertices(final Direction direction,
			final String... labels) {
		return new Iterable<Vertex>() {
			@Override
			public Iterator<Vertex> iterator() {
				final Iterator<Edge> edges = getEdges(direction, labels)
						.iterator();

				return new Iterator<Vertex>() {

					@Override
					public boolean hasNext() {
						return edges.hasNext();
					}

					@Override
					public Vertex next() {
						Edge e = edges.next();
						switch (direction) {
						case IN:
							// no break intentional
						case OUT:
							return e.getVertex(direction);
						case BOTH:
							Vertex v = e.getVertex(Direction.IN);
							if (this.equals(v)) {
								return e.getVertex(Direction.OUT);
							} else {
								return v;
							}
						default:
							throw new RuntimeException("Unexpected direction: "
									+ direction);
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			};
		};
	}

	@Override
	public VertexQuery query() {
		return new DefaultVertexQuery(getParent().getVertex(getId()));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		inEdges.clear();
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			String eid = in.readUTF();
			String label = in.readUTF();
			String src = in.readUTF();
			MapReduceEdge mre = new MapReduceEdge(parent, eid, src, label, id);
			inEdges.add(mre);
		}

		outEdges.clear();
		count = in.readInt();
		for (int i = 0; i < count; i++) {
			String eid = in.readUTF();
			String label = in.readUTF();
			String dest = in.readUTF();
			MapReduceEdge mre = new MapReduceEdge(parent, eid, id, label, dest);
			outEdges.add(mre);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(inEdges.size());
		for (Edge e : inEdges) {
			out.writeUTF(e.getId().toString());
			out.writeUTF(e.getLabel());
			MapReduceEdge mre = (MapReduceEdge) e;
			out.writeUTF(mre.getVertexId(Direction.OUT));
		}
		out.writeInt(outEdges.size());
		for (Edge e : outEdges) {
			out.writeUTF(e.getId().toString());
			out.writeUTF(e.getLabel());
			MapReduceEdge mre = (MapReduceEdge) e;
			out.writeUTF(mre.getVertexId(Direction.IN));
		}
	}

}
