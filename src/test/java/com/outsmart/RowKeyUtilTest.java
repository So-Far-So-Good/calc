package com.outsmart;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Vadim Bobrov
 */
public class RowKeyUtilTest {

	String customer = "this is my customer name";
	String location = "this is my location";
	String wireid = "this is my wireid";

	byte[] rowkey = RowKeyUtil.createRowKey(customer, location, wireid, 111);

	@Test
	public void testGetCustomerHash() throws Exception {
		byte[] customerHash = RowKeyUtil.getHash(customer);
		byte[] returnedCustomerHash = RowKeyUtil.getCustomerHash(rowkey);

		for(byte b : customerHash)
			System.out.print(b + " ");

		System.out.println();

		for(byte b : returnedCustomerHash)
			System.out.print(b + " ");

		assertArrayEquals(customerHash, returnedCustomerHash);
	}

	@Test
	public void testGetLocationHash() throws Exception {
		assertArrayEquals(RowKeyUtil.getHash(location), RowKeyUtil.getLocationHash(rowkey));
	}

	@Test
	public void testGetWireIdHash() throws Exception {
		assertArrayEquals(RowKeyUtil.getHash(wireid), RowKeyUtil.getWireIdHash(rowkey));
	}

}
