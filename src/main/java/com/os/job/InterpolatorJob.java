package com.os.job;

import com.os.RowKeyUtil;
import com.os.Settings;
import com.os.interpolation.Interpolator;
import com.os.interpolation.SlidingInterpolatorImpl;
import com.os.measurement.TimedValue;
import com.os.measurement.TimedValueWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Vadim Bobrov
 */
public class InterpolatorJob {

	public static class MyMapper extends TableMapper<
			ImmutableBytesWritable,		// customer, location, wireid
			TimedValueWritable          // timestamp and value to be interpolated
			> {

		private int numRecords = 0;

		@Override
		public void map(ImmutableBytesWritable rowkey, Result rowvalue, Context context) throws IOException, InterruptedException {

			byte[] customerHash = RowKeyUtil.getCustomerHash(rowkey.get());
			byte[] locationHash = RowKeyUtil.getLocationHash(rowkey.get());
			byte[] wireidHash = RowKeyUtil.getWireIdHash(rowkey.get());

			ImmutableBytesWritable wireKey = new ImmutableBytesWritable(Bytes.add(customerHash, locationHash, wireidHash));

			long timestamp = RowKeyUtil.getTimestamp(rowkey.get());
			double energy = Bytes.toDouble(rowvalue.getValue(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.ValueQualifierName)));

			try {
				context.write(wireKey, new TimedValueWritable(timestamp, energy));
			} catch (InterruptedException e) {
				throw new IOException(e);
			}

			if ((numRecords++ % 10000) == 0)
				context.setStatus("mapper processed " + numRecords + " records so far");

		}

	}

	protected static enum InterpolatorValue {
		INCOMING_VALUES,
		INCOMING_KEYS
	}

	public static class MyReducer extends TableReducer<ImmutableBytesWritable, TimedValueWritable, ImmutableBytesWritable> {

		private Log log = LogFactory.getLog(MyReducer.class);

		@Override
		public void reduce(ImmutableBytesWritable wireKey, Iterable<TimedValueWritable> timedValues, Context context) throws IOException, InterruptedException {

			Interpolator itp = new SlidingInterpolatorImpl();

			List<TimedValue> tvalues = new ArrayList<TimedValue>();

			for(TimedValueWritable tvw : timedValues) {
				//log.info("received value " + tvw.getTimedValue().timestamp());
				tvalues.add(tvw.getTimedValue());
			}
			Collections.sort(tvalues);

			//context.getCounter(InterpolatorValue.INCOMING_KEYS).increment(1);

			for(TimedValue tvw : tvalues) {
				//context.getCounter(InterpolatorValue.INCOMING_VALUES).increment(1);
				List<TimedValue> interpolated = itp.offer (tvw);

				for(TimedValue tv : interpolated) {
					log.info("\tinterpolated value " + tv.timestamp());
					// create key with interpolated timestamp
					ImmutableBytesWritable itpKey = new ImmutableBytesWritable(Bytes.add(wireKey.get(), Bytes.toBytes(Long.MAX_VALUE - tv.timestamp())));

					Put put = new Put(itpKey.get());
					put.add(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.ValueQualifierName), Bytes.toBytes(tv.value()));

					context.write(itpKey, put);
				}
			}

		}
	}


	public static Job getJob(Configuration conf)  throws Exception {
		Job job = new Job(conf, "InterpolatorJob");
		job.setJarByClass(InterpolatorJob.class);

		byte[] startRowKey = RowKeyUtil.createRowKey(conf.get("customer"), conf.get("location"), conf.get("wireid"), conf.getLong("from", 0));
		byte[] endRowKey = RowKeyUtil.createRowKey(conf.get("customer"), conf.get("location"), conf.get("wireid"), conf.getLong("to", Long.MAX_VALUE));

		Scan scan = new Scan(startRowKey, endRowKey);
		scan.setCaching(500);        					// 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  					// don't set to true for MR jobs
		scan.addColumn(Bytes.toBytes(Settings.ColumnFamilyName), Bytes.toBytes(Settings.ValueQualifierName));

		//TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.initTableMapperJob(
				Settings.TableName,						// input HBase table name
				scan,             						// Scan instance to control CF and attribute selection
				MyMapper.class,   						// mapper
				ImmutableBytesWritable.class,	        // mapper output key
				TimedValueWritable.class,        			// mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(
				Settings.MinuteInterpolaedTableName,	// output HBase table name
				MyReducer.class,
				job);

		job.setNumReduceTasks(1);   					// at least one, adjust as required
		return job;
	}


}