package edu.jhuapl.tinkerpop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;

public class VertexInputFormat extends InputFormatBase<Text, Vertex> {
	static AccumuloGraphConfiguration conf;
	
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
		
			try {
				conf = new AccumuloGraphConfiguration().name(VertexInputFormat.getInputTableName(attempt).split("_")[0]);
				conf.zkHosts(VertexInputFormat.getInstance(attempt).getZooKeepers());
				conf.instance(VertexInputFormat.getInstance(attempt).getInstanceName());
				conf.user(VertexInputFormat.getPrincipal(attempt));
				conf.password(VertexInputFormat.getToken(attempt));
				parent = AccumuloGraph.open(conf);
			} catch (AccumuloException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

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
				currentV = vertex;
				return true;
			}
			return false;
		}

	}
	
	public static void setAccumuloGraphConfiguration(Job job, AccumuloGraphConfiguration cfg) throws AccumuloSecurityException{
		
		VertexInputFormat.setConnectorInfo(job, cfg.getUser(), new PasswordToken(cfg.getPassword()));
		VertexInputFormat.setInputTableName(job,cfg.getVertexTable());
		VertexInputFormat.setZooKeeperInstance(job, cfg.getInstance(), cfg.getZooKeeperHosts());
		
	}

}
