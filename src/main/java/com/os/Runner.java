package com.os;

import com.os.job.InterpolatorJob;
import com.os.job.RollupJob;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Vadim Bobrov
 */
public class Runner extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", Settings.HBaseHost);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		CommandLine cmd = parseArgs(otherArgs);

		String table = cmd.getOptionValue("tn");

		String customer = cmd.hasOption("c") ? cmd.getOptionValue("c") : "";
		String location = cmd.hasOption("l") ? cmd.getOptionValue("l") : "";

		long from = cmd.hasOption("f") ? Long.valueOf(cmd.getOptionValue("f")) : 0L;
		long to = cmd.hasOption("t") ? Long.valueOf(cmd.getOptionValue("t")) : Long.MAX_VALUE;


		System.out.println("customer: " + customer);
		System.out.println("location: " + location);
		System.out.println("from: " + from);
		System.out.println("to: " + to);

		conf.set("table", table);
		conf.set("customer", customer);
		conf.set("location", location);
		conf.setLong("from", from);
		conf.setLong("to", to);

		boolean res = InterpolatorJob.getJob(conf).waitForCompletion(true);
		if(res)
			res = RollupJob.getJob(conf).waitForCompletion(true);

		System.exit(res ? 0 : 1);
		return 0;
	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("tn", "table", true, "table to compact");
		o.setArgName("table-name");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("c", "customer", true, "customer name");
		o.setArgName("customer");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("l", "location", true, "customer location");
		o.setArgName("location");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("f", "from", true, "from time (in millis since epoch)");
		o.setArgName("from");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("t", "to", true, "until time (in millis since epoch)");
		o.setArgName("to");
		o.setRequired(false);
		options.addOption(o);

		options.addOption("d", "debug", false, "switch on DEBUG log level");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("hadoop jar calc.jar ", options, true);
			System.exit(-1);
		}
		return cmd;
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Runner(), args);
		System.exit(res);
	}

}
