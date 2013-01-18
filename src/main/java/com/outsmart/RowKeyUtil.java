package com.outsmart;

import org.apache.hadoop.hbase.util.Bytes;
import java.security.MessageDigest;

/**
 * @author Vadim Bobrov
 */
public class RowKeyUtil {

	static final private int SIZEOF_STRING = 16;

	/**
	 * create rowkey using customer, location, wireid and reversed timestamp
	 * @param customer
	 * @param location
	 * @param wireid
	 * @param timestamp
	 * @return
	 */
	static public byte[] createRowKey(String customer, String location, String wireid, long timestamp) {

		byte[] rowkey = new byte[SIZEOF_STRING + SIZEOF_STRING + SIZEOF_STRING + Bytes.SIZEOF_LONG];

		Bytes.putBytes(rowkey, 0, getHash(customer), 0, SIZEOF_STRING);
		Bytes.putBytes(rowkey, SIZEOF_STRING, getHash(location), 0, SIZEOF_STRING);
		Bytes.putBytes(rowkey, SIZEOF_STRING + SIZEOF_STRING, getHash(wireid), 0, SIZEOF_STRING);

		long reverseTimestamp = Long.MAX_VALUE - timestamp;
		Bytes.putLong(rowkey, SIZEOF_STRING + SIZEOF_STRING + SIZEOF_STRING, reverseTimestamp);

		return rowkey;
	}

	/**
	 * create partial rowkey using only part of the entire key
	 * @param customer
	 * @param location
	 * @return
	 */
	static public byte[] createRowKeyPrefix(String customer, String location) {

		byte[] rowkey = new byte[SIZEOF_STRING + SIZEOF_STRING];

		Bytes.putBytes(rowkey, 0, getHash(customer), 0, SIZEOF_STRING);
		Bytes.putBytes(rowkey, SIZEOF_STRING, getHash(location), 0, SIZEOF_STRING);

		return rowkey;
	}

	/*
	  * get a unique (almost) hash for a string to use in row key
	   */
	static private byte[] getHash(String s) {

		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(s.getBytes());

		} catch (Exception e) {
			// ignore - should not happen
		}

		return md.digest();
	}

	/**
	 * extract timestamp from rowkey bytes
	 * @param rowkey bytes to extract from
	 * @return
	 */
	static public long getTimestamp(byte[] rowkey) {
		long reverseTimestamp = Bytes.toLong(rowkey, SIZEOF_STRING + SIZEOF_STRING + SIZEOF_STRING);
		return Long.MAX_VALUE - reverseTimestamp;
	}

}
