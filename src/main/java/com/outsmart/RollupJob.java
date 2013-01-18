package com.outsmart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Vadim Bobrov
 */
public class RollupJob extends Configured implements Tool {

	public static class MapClass extends TableMapper<ImmutableBytesWritable, Result> {

		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException, InterruptedException {

			for(KeyValue value: columns.list()) {
				if(value.getValue().length > 0)
					context.write(row, new Result());   // write something into result
			}

		}

	}


	public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {

		String tableName = args[0];
		Job job = new Job(conf, "rowcounter_" + tableName);
		job.setJarByClass(RollupJob.class); //TODO: see what this is


		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("power"));

		job.setOutputFormatClass(NullOutputFormat.class); //TODO: see what this is
		TableMapReduceUtil.initTableMapperJob(tableName, scan, MapClass.class, ImmutableBytesWritable.class, Result.class, job);
		job.setNumReduceTasks(0);
		return job;
	}


	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "RollupJob");
		job.setJarByClass(RollupJob.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RollupJob(), args);
		System.exit(res);
	}
}