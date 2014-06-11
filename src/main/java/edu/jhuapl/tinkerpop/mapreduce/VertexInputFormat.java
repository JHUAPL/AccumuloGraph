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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;

public class VertexInputFormat extends InputFormatBase<Text, Vertex> {

	@Override
	public RecordReader<Text, Vertex> createRecordReader(InputSplit split,
			TaskAttemptContext attempt) throws IOException,
			InterruptedException {
		return new VertexRecordReader();
	}

	private class VertexRecordReader extends RecordReaderBase<Text, Vertex> {

		RowIterator rowIterator;
		AccumuloGraph parent;
		
		VertexRecordReader() {
		}

		@Override
		public void initialize(InputSplit inSplit, TaskAttemptContext attempt)
				throws IOException {
			super.initialize(inSplit, attempt);
			rowIterator = new RowIterator(scannerIterator);
			
			currentK = new Text();

			// TODO - initialize parent based on attempt config!

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (rowIterator.hasNext()) {
				Iterator<Entry<Key, Value>> it = rowIterator.next();

				MapReduceVertex vertex = new MapReduceVertex(parent);
				while (it.hasNext()) {
					Entry<Key, Value> entry = it.next();
					numKeysRead++;

					currentKey = entry.getKey();
					String vid = currentKey.getRow().toString();
					String colf = currentKey.getColumnFamily().toString();
					switch (colf) {
					case AccumuloGraph.SLABEL:
						currentK.set(vid);
						vertex.prepareId(vid);
						break;
					case AccumuloGraph.SINEDGE:
						String[] parts = currentKey.getColumnQualifier()
								.toString().split(AccumuloGraph.IDDELIM);
						String label = new String(entry.getValue().get());
						vertex.prepareEdge(parts[1], parts[0], label, vid);
						break;
					case AccumuloGraph.SOUTEDGE:
						parts = currentKey.getColumnQualifier().toString()
								.split(AccumuloGraph.IDDELIM);
						label = new String(entry.getValue().get());
						vertex.prepareEdge(parts[1], vid, label, parts[0]);
						break;
					default:
						String propertyKey = currentKey.getColumnFamily()
								.toString();
						Object propertyValue = AccumuloByteSerializer
								.desserialize(entry.getValue().get());
						vertex.prepareProperty(propertyKey, propertyValue);
					}
				}
				return true;
			}
			return false;
		}

	}

}
