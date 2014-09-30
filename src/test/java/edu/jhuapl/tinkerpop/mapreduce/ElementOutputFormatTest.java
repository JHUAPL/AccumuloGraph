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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;

public class ElementOutputFormatTest {
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  private static class MRTester extends Configured implements Tool {

    private static class TestVertexMapper extends Mapper<Text,Vertex,NullWritable,Element> {
      int count = 0;

      @Override
      protected void map(Text k, Vertex v, Context context) throws IOException, InterruptedException {
        try {
          assertEquals(k.toString(), v.getId().toString());

          v.setProperty("NAME", "BANANA" + v.getId());
          context.write(NullWritable.get(), v);
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

      setConf(new Configuration());

      getConf().set("fs.default.name", "local");

      Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      AccumuloGraphConfiguration cfg = new AccumuloGraphConfiguration().setInstanceName("_mapreduce_instance2").setUser("root").setPassword("".getBytes())
          .setGraphName("_mapreduce_table_2").setInstanceType(InstanceType.Mock).setCreate(true);
      job.setInputFormatClass(EdgeInputFormat.class);

      ElementOutputFormat.setAccumuloGraphConfiguration(job, cfg);

      job.setMapperClass(TestVertexMapper.class);

      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(Element.class);
      job.setOutputFormatClass(ElementOutputFormat.class);

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
    final String INSTANCE_NAME = "_mapreduce_instance2";
    final String TEST_TABLE_1 = "_mapreduce_table_2";

    if (!System.getProperty("os.name").startsWith("Windows")) {
      Graph g = GraphFactory.open(new AccumuloGraphConfiguration().setInstanceName(INSTANCE_NAME).setUser("root").setPassword("".getBytes())
          .setGraphName(TEST_TABLE_1).setInstanceType(InstanceType.Mock).setCreate(true).getConfiguration());
      for (int i = 0; i < 100; i++) {
        g.addVertex(i + "");
      }
      assertEquals(0, MRTester.main(new String[] {}));
      assertNull(e1);
      assertNull(e2);
      assertEquals(g.getVertex("1").getProperty("NAME"), "BANANA1");
    }
  }
}
