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

import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraph;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class VertexInputFormat extends InputFormatBase<Text,Vertex> {
  static AccumuloGraphConfiguration conf;

  private static final String PREFIX = VertexInputFormat.class.getSimpleName()+".";
  private static final String GRAPH_NAME = PREFIX+"graph.name";

  @Override
  public RecordReader<Text,Vertex> createRecordReader(InputSplit split,
      TaskAttemptContext attempt) throws IOException, InterruptedException {
    return new VertexRecordReader();
  }

  private class VertexRecordReader extends RecordReaderBase<Text,Vertex> {

    RowIterator rowIterator;
    AccumuloGraph parent;

    VertexRecordReader() {}

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {

      super.initialize(inSplit, attempt);
      rowIterator = new RowIterator(scannerIterator);

      currentK = new Text();

      try {
        conf = new AccumuloGraphConfiguration();
        conf.setZooKeeperHosts(VertexInputFormat.getInstance(attempt).getZooKeepers());
        conf.setInstanceName(VertexInputFormat.getInstance(attempt).getInstanceName());
        conf.setUser(VertexInputFormat.getPrincipal(attempt));
        conf.setPassword(VertexInputFormat.getToken(attempt));
        conf.setGraphName(attempt.getConfiguration().get(GRAPH_NAME));
        if (VertexInputFormat.getInstance(attempt) instanceof MockInstance) {
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
        Iterator<Entry<Key,Value>> it = rowIterator.next();

        MapReduceVertex vertex = new MapReduceVertex(parent);
        while (it.hasNext()) {
          Entry<Key,Value> entry = it.next();
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
              String[] parts = currentKey.getColumnQualifier().toString().split(AccumuloGraph.IDDELIM);
              String label = new String(entry.getValue().get());
              vertex.prepareEdge(parts[1], parts[0], label, vid);
              break;
            case AccumuloGraph.SOUTEDGE:
              parts = currentKey.getColumnQualifier().toString().split(AccumuloGraph.IDDELIM);
              label = new String(entry.getValue().get());
              vertex.prepareEdge(parts[1], vid, label, parts[0]);
              break;
            default:
              String propertyKey = currentKey.getColumnFamily().toString();
              Object propertyValue = AccumuloByteSerializer.deserialize(entry.getValue().get());
              vertex.prepareProperty(propertyKey, propertyValue);
          }
        }
        currentV = vertex;
        return true;
      }
      return false;
    }

  }

  public static void setAccumuloGraphConfiguration(Job job, AccumuloGraphConfiguration cfg) throws AccumuloSecurityException {

    VertexInputFormat.setConnectorInfo(job, cfg.getUser(), new PasswordToken(cfg.getPassword()));
    VertexInputFormat.setInputTableName(job, cfg.getVertexTableName());
    if (cfg.getInstanceType().equals(InstanceType.Mock)) {
      VertexInputFormat.setMockInstance(job, cfg.getInstanceName());
    } else {
      VertexInputFormat.setZooKeeperInstance(job, cfg.getInstanceName(), cfg.getZooKeeperHosts());
    }
    job.getConfiguration().set(GRAPH_NAME, cfg.getGraphName());
  }

}
