package com.outsmart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Vadim Bobrov
 */
public class RollupJob extends Configured implements Tool {

	public static class MyMapper extends TableMapper<
			ImmutableBytesWritable,		// customer, location, timestamp
			IntWritable 				// value for the wireid to be added up
			> {

		private int numRecords = 0;
		private final IntWritable ONE = new IntWritable(1);
		private Text text = new Text();

		@Override
		public void map(ImmutableBytesWritable rowkey, Result rowvalue, Context context) throws IOException, InterruptedException {

			String val = new String(rowvalue.getValue(Bytes.toBytes("cf"), Bytes.toBytes("attr1")));
			text.set(val);     // we can only emit Writables...

			//context.write(text, ONE);



			// extract userKey from the compositeKey (userId + counter)
			//RowKeyUtil.getTimestamp()


			ImmutableBytesWritable userKey = new ImmutableBytesWritable(rowkey.get(), 0, Bytes.SIZEOF_INT);
			try {
				context.write(userKey, ONE);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}


			numRecords++;
			if ((numRecords % 10000) == 0) {
				context.setStatus("mapper processed " + numRecords + " records so far");
			}

		}

	}


	public static class MyTableReducer extends TableReducer<IntWritable, IntWritable, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("count"), Bytes.toBytes(i));

			context.write(null, put);
		}
	}


	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "RollupJob");
		job.setJarByClass(RollupJob.class);

		Scan scan = new Scan();
		scan.setCaching(500);        					// 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  					// don't set to true for MR jobs
		scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("power"));

		TableMapReduceUtil.initTableMapperJob(
				"ismt",        							// input HBase table name
				scan,             						// Scan instance to control CF and attribute selection
				MyMapper.class,   						// mapper
				IntWritable.class,			           // mapper output key  												timestamp
				IntWritable.class,        				// mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(
				"rsmt",                                    // output HBase table name
				MyTableReducer.class,
				job);

		job.setNumReduceTasks(1);   					// at least one, adjust as required
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RollupJob(), args);
		System.exit(res);
	}
}