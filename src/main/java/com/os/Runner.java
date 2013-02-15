package com.os;

import com.os.job.InterpolatorJob;
import com.os.job.RollupJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Vadim Bobrov
 */
public class Runner extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		String customer = args[0];
		String location = args[1];

		long from = Long.valueOf(args[2]);
		long to = Long.valueOf(args[3]);
		String wireid = args[4];

		System.out.println("customer: " + customer);
		System.out.println("location: " + location);
		System.out.println("wireid: " + wireid);
		System.out.println("from: " + from);
		System.out.println("to: " + to);

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", Settings.HBaseHost);

		conf.set("customer", customer);
		conf.set("location", location);
		conf.set("wireid", wireid);
		conf.setLong("from", from);
		conf.setLong("to", to);

		boolean res = InterpolatorJob.getJob(conf).waitForCompletion(true);
		if(res)
			res = RollupJob.getJob(conf).waitForCompletion(true);

		System.exit(res ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Runner(), args);
		System.exit(res);
	}

}
