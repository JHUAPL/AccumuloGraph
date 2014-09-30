package edu.jhuapl.tinkerpop.mapreduce;

import java.io.IOException;
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
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class ElementOutputFormat extends OutputFormat<NullWritable,Element> {

  @Override
  public RecordWriter<NullWritable,Element> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new ElementRecordWriter(context);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

  }

  public static void setAccumuloConfiguration(Job job, AccumuloGraphConfiguration acc) {
    acc.validate();
    Configuration jobconf = job.getConfiguration();

    jobconf.set(AccumuloGraphConfiguration.USER, acc.getUser());
    jobconf.set(AccumuloGraphConfiguration.PASSWORD, new String(acc.getPassword().array()));
    jobconf.set(AccumuloGraphConfiguration.GRAPH_NAME, acc.getName());
    jobconf.set(AccumuloGraphConfiguration.INSTANCE, acc.getInstance());
    jobconf.set(AccumuloGraphConfiguration.INSTANCE_TYPE, acc.getInstanceType().toString());
    jobconf.set(AccumuloGraphConfiguration.ZK_HOSTS, acc.getZooKeeperHosts());
  }

  /**
   * @see AccumuloOutputFormat
   */
  // TODO I think we can implement this to provide a little more robustness.
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new NullOutputFormat<Text,Mutation>().getOutputCommitter(context);
  }

  class ElementRecordWriter extends RecordWriter<NullWritable,Element> {
    AccumuloGraphConfiguration config;

    protected ElementRecordWriter(TaskAttemptContext context) {
      config = new AccumuloGraphConfiguration();
      Configuration jobconf = context.getConfiguration();
      config.setUser(jobconf.get(AccumuloGraphConfiguration.USER));
      config.setPassword(jobconf.get(AccumuloGraphConfiguration.PASSWORD));
      config.setGraphName(jobconf.get(AccumuloGraphConfiguration.GRAPH_NAME));
      config.setInstanceName(jobconf.get(AccumuloGraphConfiguration.INSTANCE));
      config.setInstanceType(InstanceType.valueOf(jobconf.get(AccumuloGraphConfiguration.INSTANCE_TYPE)));
      config.setZookeeperHosts(jobconf.get(AccumuloGraphConfiguration.ZK_HOSTS));

    }

    BatchWriter bw;

    @Override
    public void write(NullWritable key, Element value) throws IOException, InterruptedException {
      MapReduceElement ele = (MapReduceElement) value;
      try {
        if (bw == null) {
          if (ele instanceof MapReduceVertex) {
            bw = config.getConnector().createBatchWriter(config.getVertexTable(), config.getBatchWriterConfig());
          } else {
            bw = config.getConnector().createBatchWriter(config.getEdgeTable(), config.getBatchWriterConfig());
          }
        }

        Mutation mut = new Mutation(ele.id);
        for (Entry<String,Object> map : ele.getNewProperties().entrySet()) {
          mut.put(map.getKey().getBytes(), "".getBytes(), AccumuloByteSerializer.serialize(map.getValue()));
        }

        bw.addMutation(mut);
      } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      if (bw != null) {
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          e.printStackTrace();
        }
      }
    }

  }

}
