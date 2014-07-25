package edu.jhuapl.tinkerpop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.tinkerpop.blueprints.Edge;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class EdgeInputFormat extends InputFormatBase<Text, Edge> {
	static AccumuloGraphConfiguration conf;
	
	@Override
	public RecordReader<Text, Edge> createRecordReader(InputSplit split,
			TaskAttemptContext attempt) throws IOException,
			InterruptedException {
		return new EdgeRecordReader();
	}

	private class EdgeRecordReader extends RecordReaderBase<Text, Edge> {

		RowIterator rowIterator;
		AccumuloGraph parent;
		
		EdgeRecordReader() {
		}

		@Override
		public void initialize(InputSplit inSplit, TaskAttemptContext attempt)
				throws IOException {
			
			super.initialize(inSplit, attempt);
			rowIterator = new RowIterator(scannerIterator);
		
			currentK = new Text();
		
			try {
				conf = new AccumuloGraphConfiguration();
				conf.setZookeeperHosts(EdgeInputFormat.getInstance(attempt).getZooKeepers());
				conf.setInstanceName(EdgeInputFormat.getInstance(attempt).getInstanceName());
				conf.setUser(EdgeInputFormat.getPrincipal(attempt));
				conf.setPassword(EdgeInputFormat.getToken(attempt));
				System.out.println(conf.getPassword());
				conf.setGraphName(attempt.getConfiguration().get(AccumuloGraphConfiguration.GRAPH_NAME));
				if(EdgeInputFormat.getInstance(attempt) instanceof MockInstance){
					conf.setInstanceType(InstanceType.Mock);
				}
				parent = AccumuloGraph.open(conf.getConfiguration());
			} catch (AccumuloException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (rowIterator.hasNext()) {
				Iterator<Entry<Key, Value>> it = rowIterator.next();

				MapReduceEdge edge = new MapReduceEdge(parent);
				while (it.hasNext()) {
					Entry<Key, Value> entry = it.next();
					numKeysRead++;

					currentKey = entry.getKey();
					String eid = currentKey.getRow().toString();
					String colf = currentKey.getColumnFamily().toString();
					switch (colf) {
					case AccumuloGraph.SLABEL:
						currentK.set(eid);
						edge.prepareId(eid);
						String[] ids = currentKey.getColumnQualifier().toString().split(parent.IDDELIM);
						edge.setSourceId(ids[1]);
						edge.setDestId(ids[0]);
						edge.setLabel( AccumuloByteSerializer
								.desserialize(entry.getValue().get()).toString());
						break;
					default:
						String propertyKey = currentKey.getColumnFamily()
								.toString();
						Object propertyValue = AccumuloByteSerializer
								.desserialize(entry.getValue().get());
						edge.prepareProperty(propertyKey, propertyValue);
					}
				}
				currentV = edge;
				return true;
			}
			return false;
		}

	}
	
	public static void setAccumuloGraphConfiguration(Job job, AccumuloGraphConfiguration cfg) throws AccumuloSecurityException{
		
		EdgeInputFormat.setConnectorInfo(job, cfg.getUser(), new PasswordToken(cfg.getPassword()));
		EdgeInputFormat.setInputTableName(job,cfg.getEdgeTable());
		if(cfg.getInstanceType().equals(InstanceType.Mock)){
		    EdgeInputFormat.setMockInstance(job, cfg.getInstance());
		}else{
		    EdgeInputFormat.setZooKeeperInstance(job, cfg.getInstance(), cfg.getZooKeeperHosts());
		}
		job.getConfiguration().set(AccumuloGraphConfiguration.GRAPH_NAME, cfg.getName());
		
	}

}
