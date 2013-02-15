package com.os;

/**
 * @author Vadim Bobrov
 */
public class Settings {

	/*
	  You may need to find a sweet spot between a low number of RPCs and the memory
	  used on the client and server. Setting the scanner caching higher will improve scanning
		performance most of the time, but setting it too high can have adverse effects as well:
		each call to next() will take longer as more data is fetched and needs to be transported
		  to the client, and once you exceed the maximum heap the client process has available
		it may terminate with an OutOfMemoryException.
	  When the time taken to transfer the rows to the client, or to process the
		data on the client, exceeds the configured scanner lease threshold, you
	  will end up receiving a lease expired error, in the form of a Scan
		nerTimeoutException being thrown.
	  */
	static public final int ScanCacheSize = 1000;     		   			// how many rows are retrieved with every RPC call


	static public final String  TableName = "msmt";              		// table for actual measurements
	static public final String MinuteInterpolaedTableName = "ismt";    // table for minute interpolation
	static public final String RollupTableName = "rsmt";    			// table for minute rollup by location


	static public final String ColumnFamilyName = "d";      			// stands for data
	static public final String ValueQualifierName = "v";   			// stands for value
	static public final String HBaseHost = "node0";
}
