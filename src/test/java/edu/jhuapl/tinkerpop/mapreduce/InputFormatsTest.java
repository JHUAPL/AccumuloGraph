package edu.jhuapl.tinkerpop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class InputFormatsTest {

  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  private static class MRTester extends Configured implements Tool {

    private static class TestEdgeMapper extends Mapper<Text,Edge,NullWritable,NullWritable> {
      int count = 0;

      @Override
      protected void map(Text k, Edge v, Context context) throws IOException, InterruptedException {
        try {
          assertEquals(k.toString(), v.getId().toString());
          MapReduceEdge e = (MapReduceEdge) v;
          assertEquals(e.getVertexId(Direction.OUT) + "a", e.getVertexId(Direction.IN));
        } catch (AssertionError e) {
          e1 = e;
        }
        count++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    private static class TestVertexMapper extends Mapper<Text,Vertex,NullWritable,NullWritable> {
      // Key key = null;
      int count = 0;

      @Override
      protected void map(Text k, Vertex v, Context context) throws IOException, InterruptedException {
        try {
          assertEquals(k.toString(), v.getId().toString());
        } catch (AssertionError e) {
          e1 = e;
        }
        count++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 5) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table> <instanceName> <edge?>");
      }

      String user = args[0];
      String pass = args[1];
      String table = args[2];

      String instanceName = args[3];

      setConf(new Configuration());
      // getConf().set("mapred.job.tracker", "local");
      getConf().set("fs.default.name", "local");

      Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration().setInstanceName(instanceName).setUser(user).setPassword(pass.getBytes())
          .setGraphName(table).setInstanceType(InstanceType.Mock).setCreate(true);
      if (Boolean.parseBoolean(args[4])) {

        job.setInputFormatClass(EdgeInputFormat.class);

        EdgeInputFormat.setAccumuloGraphConfiguration(job, cfg);

        job.setMapperClass(TestEdgeMapper.class);
      } else {
        job.setInputFormatClass(VertexInputFormat.class);

        VertexInputFormat.setAccumuloGraphConfiguration(job, cfg);
        job.setMapperClass(TestVertexMapper.class);
      }

      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static int main(String[] args) throws Exception {
      return ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args);
    }
  }

  @Test
  public void testVertexInputMap() throws Exception {
    final String INSTANCE_NAME = "_mapreduce_instance";
    final String TEST_TABLE_NAME = "_mapreduce_table_vertexInputMap";

    if (!System.getProperty("os.name").startsWith("Windows")) {
      Graph g = GraphFactory.open(new AccumuloGraphConfiguration().setInstanceName(INSTANCE_NAME)
          .setUser("root").setPassword("".getBytes())
          .setGraphName(TEST_TABLE_NAME).setInstanceType(InstanceType.Mock)
          .setCreate(true).getConfiguration());

      for (int i = 0; i < 100; i++) {
        g.addVertex(i + "");
      }

      assertEquals(0, MRTester.main(new String[]{"root", "",
          TEST_TABLE_NAME, INSTANCE_NAME, "false"}));
      assertNull(e1);
      assertNull(e2);

      g.shutdown();
    }
  }

  @Test
  public void testEdgeInputMap() throws Exception {
    final String INSTANCE_NAME = "_mapreduce_instance";
    final String TEST_TABLE_NAME = "_mapreduce_table_edgeInputMap";

    if (!System.getProperty("os.name").startsWith("Windows")) {
      Graph g = GraphFactory.open(new AccumuloGraphConfiguration().setInstanceName(INSTANCE_NAME)
          .setUser("root").setPassword("".getBytes())
          .setGraphName(TEST_TABLE_NAME).setInstanceType(InstanceType.Mock)
          .setAutoFlush(true).setCreate(true).getConfiguration());

      for (int i = 0; i < 100; i++) {
        Vertex v1 = g.addVertex(i+"");
        Vertex v2 = g.addVertex(i+"a");
        g.addEdge(null, v1, v2, "knows");
      }

      assertEquals(0, MRTester.main(new String[]{"root", "",
          TEST_TABLE_NAME, INSTANCE_NAME, "true"}));
      assertNull(e1);
      assertNull(e2);

      g.shutdown();
    }
  }

}
