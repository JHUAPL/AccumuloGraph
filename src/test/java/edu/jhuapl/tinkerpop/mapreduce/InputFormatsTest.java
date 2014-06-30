package edu.jhuapl.tinkerpop.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.Vertex;

import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration;
import edu.jhuapl.tinkerpop.AccumuloGraphConfiguration.InstanceType;


public class InputFormatsTest {

	private static AssertionError e1 = null;
	private static AssertionError e2 = null;

	private static class MRTester extends Configured implements Tool {
		private static class TestMapper extends
				Mapper<Text, Vertex, NullWritable, NullWritable> {
			// Key key = null;
			int count = 0;

			@Override
			protected void map(Text k, Vertex v, Context context)
					throws IOException, InterruptedException {
				try {
					assertEquals(k.toString(), v.getId().toString());
				} catch (AssertionError e) {
					e1 = e;
				}
				count++;
			}

			@Override
			protected void cleanup(Context context) throws IOException,
					InterruptedException {
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
				throw new IllegalArgumentException(
						"Usage : "
								+ MRTester.class.getName()
								+ " <user> <pass> <table> <instanceName> <inputFormatClass>");
			}

			String user = args[0];
			String pass = args[1];
			String table = args[2];

			String instanceName = args[3];
			String inputFormatClassName = args[4];
			@SuppressWarnings("unchecked")
			Class<? extends InputFormat<?, ?>> inputFormatClass = (Class<? extends InputFormat<?, ?>>) Class
					.forName(inputFormatClassName);

			@SuppressWarnings("deprecation")
			Job job = new Job(getConf(), this.getClass().getSimpleName() + "_"
					+ System.currentTimeMillis());
			job.setJarByClass(this.getClass());

			job.setInputFormatClass(inputFormatClass);

			VertexInputFormat.setAccumuloGraphConfiguration(job,
					new AccumuloGraphConfiguration().instance(instanceName)
							.user(user).password(pass.getBytes()).name(table)
							.instanceType(InstanceType.Mock));
			job.setMapperClass(TestMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputFormatClass(NullOutputFormat.class);

			job.setNumReduceTasks(0);

			job.waitForCompletion(true);

			return job.isSuccessful() ? 0 : 1;
		}

		public static int main(String[] args) throws Exception {
			return ToolRunner.run(CachedConfiguration.getInstance(),
					new MRTester(), args);
		}
	}

	@Test
	public void testMap() throws Exception {
		final String INSTANCE_NAME = "_mapreduce_instance";
		final String TEST_TABLE_1 = "_mapreduce_table_1";

		if (!System.getProperty("os.name").startsWith("Windows")) {
			Graph g = GraphFactory.open(new AccumuloGraphConfiguration()
					.instance("_mapreduce_instance").user("root")
					.password("".getBytes()).name("_mapreduce_table_1")
					.instanceType(InstanceType.Mock));
			for (int i = 0; i < 100; i++) {
				g.addVertex(i + "");
			}
			assertEquals(0,
					MRTester.main(new String[] { "root", "", TEST_TABLE_1,
							INSTANCE_NAME,
							VertexInputFormat.class.getCanonicalName() }));
			assertNull(e1);
			assertNull(e2);
		}
	}

}
