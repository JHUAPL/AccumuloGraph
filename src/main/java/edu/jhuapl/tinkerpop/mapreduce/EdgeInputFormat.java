package edu.jhuapl.tinkerpop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.Constants;

public class EdgeInputFormat extends InputFormatBase<Text,Edge> {

  private static final String PREFIX = EdgeInputFormat.class.getSimpleName()+".";
  private static final String GRAPH_NAME = PREFIX+"graph.name";

  static AccumuloGraphConfiguration conf;

  @Override
  public RecordReader<Text,Edge> createRecordReader(InputSplit split, TaskAttemptContext attempt) throws IOException, InterruptedException {
    return new EdgeRecordReader();
  }

  private class EdgeRecordReader extends RecordReaderBase<Text,Edge> {

    RowIterator rowIterator;
    AccumuloGraph parent;

    EdgeRecordReader() {}

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {

      super.initialize(inSplit, attempt);
      rowIterator = new RowIterator(scannerIterator);

      currentK = new Text();

      try {
        conf = new AccumuloGraphConfiguration();
        conf.setZooKeeperHosts(EdgeInputFormat.getInstance(attempt).getZooKeepers());
        conf.setInstanceName(EdgeInputFormat.getInstance(attempt).getInstanceName());
        conf.setUser(EdgeInputFormat.getPrincipal(attempt));
        conf.setTokenWithFallback(EdgeInputFormat.getToken(attempt));
        conf.setGraphName(attempt.getConfiguration().get(GRAPH_NAME));
        if (EdgeInputFormat.getInstance(attempt) instanceof MockInstance) {
          conf.setInstanceType(InstanceType.Mock);
        }
        parent = AccumuloGraph.open(conf.getConfiguration());
      } catch (AccumuloException e) {
        throw new AccumuloGraphException(e);
      }

    }
  
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (rowIterator.hasNext()) {
        Iterator<Entry<Key,Value>> it = rowIterator.next();

        MapReduceEdge edge = new MapReduceEdge(parent);
        while (it.hasNext()) {
          Entry<Key,Value> entry = it.next();
          numKeysRead++;

          currentKey = entry.getKey();
          String eid = currentKey.getRow().toString();
          String colf = currentKey.getColumnFamily().toString();
          switch (colf) {
            case Constants.LABEL:
              currentK.set(eid);
              edge.prepareId(eid);
              String[] ids = currentKey.getColumnQualifier().toString().split(Constants.ID_DELIM);
              edge.setSourceId(ids[1]);
              edge.setDestId(ids[0]);
              edge.setLabel(AccumuloByteSerializer.deserialize(entry.getValue().get()).toString());
              break;
            default:
              String propertyKey = currentKey.getColumnFamily().toString();
              Object propertyValue = AccumuloByteSerializer.deserialize(entry.getValue().get());
              edge.prepareProperty(propertyKey, propertyValue);
          }
        }
        currentV = edge;
        return true;
      }
      return false;
    }

  }

  public static void setAccumuloGraphConfiguration(Job job, AccumuloGraphConfiguration cfg) throws AccumuloSecurityException {

    EdgeInputFormat.setConnectorInfo(job, cfg.getUser(), new PasswordToken(cfg.getPassword()));
    EdgeInputFormat.setInputTableName(job, cfg.getEdgeTableName());
    if (cfg.getInstanceType().equals(InstanceType.Mock)) {
      EdgeInputFormat.setMockInstance(job, cfg.getInstanceName());
    } else {
      EdgeInputFormat.setZooKeeperInstance(job, cfg.getInstanceName(), cfg.getZooKeeperHosts());
    }
    job.getConfiguration().set(GRAPH_NAME, cfg.getGraphName());

  }

}
