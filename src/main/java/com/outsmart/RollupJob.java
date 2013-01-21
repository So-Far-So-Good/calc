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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Vadim Bobrov
 */
public class RollupJob extends Configured implements Tool {

	public static class MyMapper extends TableMapper<
			ImmutableBytesWritable,		// customer, location, reverse timestamp
			DoubleWritable              // value for the wireid to be added up
			> {

		private int numRecords = 0;

		@Override
		public void map(ImmutableBytesWritable rowkey, Result rowvalue, Context context) throws IOException, InterruptedException {

			byte[] customerHash = RowKeyUtil.getCustomerHash(rowkey.get());
			byte[] locationHash = RowKeyUtil.getLocationHash(rowkey.get());
			byte[] reverseTimestamp = RowKeyUtil.getTimestampAsBytes(rowkey.get());
			ImmutableBytesWritable rollupKey = new ImmutableBytesWritable(Bytes.add(customerHash, locationHash, reverseTimestamp));

			DoubleWritable energy = new DoubleWritable(Bytes.toDouble(rowvalue.getValue(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.EnergyQualifierName))));

			try {
				context.write(rollupKey, energy);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}

			if ((numRecords++ % 1000) == 0)
				context.setStatus("mapper processed " + numRecords + " records so far");

		}

	}


	public static class MyCombiner extends Reducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable, DoubleWritable> {

		@Override
		public void reduce(ImmutableBytesWritable rollupKey, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			double total = 0;
			for (DoubleWritable val : values)
				total += val.get();


			context.write(rollupKey, new DoubleWritable(total));
		}
	}

	public static class MyReducer extends TableReducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable> {

		@Override
		public void reduce(ImmutableBytesWritable rollupKey, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			double total = 0;
			for (DoubleWritable val : values)
				total += val.get();


			Put put = new Put(rollupKey.get());
			put.add(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.EnergyQualifierName), Bytes.toBytes(total));

			context.write(rollupKey, put);
		}
	}


	public int run(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "RollupJob");
		job.setJarByClass(RollupJob.class);

		Scan scan = new Scan();
		scan.setCaching(500);        					// 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  					// don't set to true for MR jobs
		scan.addColumn(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.EnergyQualifierName));

		TableMapReduceUtil.initTableMapperJob(
				Settings.MinuteInterpolaedTableName,	// input HBase table name
				scan,             						// Scan instance to control CF and attribute selection
				MyMapper.class,   						// mapper
				ImmutableBytesWritable.class,	        // mapper output key
				DoubleWritable.class,        			// mapper output value
				job);

		job.setCombinerClass(MyCombiner.class);

		TableMapReduceUtil.initTableReducerJob(
				Settings.RollupTableName,				// output HBase table name
				MyReducer.class,
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