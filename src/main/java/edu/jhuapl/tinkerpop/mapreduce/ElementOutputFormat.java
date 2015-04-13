package edu.jhuapl.tinkerpop.mapreduce;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.tinkerpop.blueprints.Element;

import edu.jhuapl.tinkerpop.AccumuloByteSerializer;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphException;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class ElementOutputFormat extends OutputFormat<NullWritable,Element> {

  private static final String PREFIX = ElementOutputFormat.class.getSimpleName()+".";
  private static final String USER = "username";
  private static final String PASSWORD = PREFIX+"password";
  private static final String GRAPH_NAME = PREFIX+"graphName";
  private static final String INSTANCE = PREFIX+"instanceName";
  private static final String INSTANCE_TYPE = PREFIX+"instanceType";
  private static final String ZK_HOSTS = PREFIX+"zookeeperHosts";

  @Override
  public RecordWriter<NullWritable,Element> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new ElementRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

  }

  public static void setAccumuloGraphConfiguration(Job job, AccumuloGraphConfiguration acc) {
    acc.validate();
    Configuration jobconf = job.getConfiguration();

    jobconf.set(USER, acc.getUser());
    jobconf.set(PASSWORD, acc.getPassword());
    jobconf.set(GRAPH_NAME, acc.getGraphName());
    jobconf.set(INSTANCE, acc.getInstanceName());
    jobconf.set(INSTANCE_TYPE, acc.getInstanceType().toString());
    if(acc.getInstanceType().equals(InstanceType.Distributed))
      jobconf.set(ZK_HOSTS, acc.getZooKeeperHosts());
  }

  /**
   * @see AccumuloOutputFormat
   */
  // TODO I think we can implement this to provide a little more robustness.
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new NullOutputFormat<Text,Mutation>().getOutputCommitter(context);
  }

  static class ElementRecordWriter extends RecordWriter<NullWritable,Element> implements Serializable{
    AccumuloGraphConfiguration config;

    public ElementRecordWriter(TaskAttemptContext context) {
      config = new AccumuloGraphConfiguration();
      Configuration jobconf = context.getConfiguration();
      config.setUser(jobconf.get(USER));
      config.setPassword(jobconf.get(PASSWORD));
      config.setGraphName(jobconf.get(GRAPH_NAME));
      config.setInstanceName(jobconf.get(INSTANCE));
      config.setInstanceType(InstanceType.valueOf(jobconf.get(INSTANCE_TYPE)));
      config.setZooKeeperHosts(jobconf.get(ZK_HOSTS));

    }

    transient BatchWriter bw;

    @Override
    public void write(NullWritable key, Element value) throws IOException, InterruptedException {
      MapReduceElement ele = (MapReduceElement) value;
      try {
        if (bw == null) {
          if (ele instanceof MapReduceVertex) {
            bw = config.getConnector().createBatchWriter(config.getVertexTableName(), config.getBatchWriterConfig());
          } else {
            bw = config.getConnector().createBatchWriter(config.getEdgeTableName(), config.getBatchWriterConfig());
          }
        }

        Mutation mut = new Mutation(ele.id);
        for (Entry<String,Object> map : ele.getNewProperties().entrySet()) {
          mut.put(map.getKey().getBytes(), "".getBytes(), AccumuloByteSerializer.serialize(map.getValue()));
        }

        bw.addMutation(mut);
      } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
        throw new AccumuloGraphException(e);
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      if (bw != null) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          throw new AccumuloGraphException(e);
        }
      }
    }

  }

}
