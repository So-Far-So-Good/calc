package com.outsmart.interpolation;

import com.outsmart.measurement.TimedValue;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Vadim Bobrov
 */
public class NQueueImplTest {

	TimedValue tv1 = new TimedValue(1,1);
	TimedValue tv2 = new TimedValue(2,1);
	TimedValue tv3 = new TimedValue(3,1);
	TimedValue tv3dup = new TimedValue(3,1);
	TimedValue tv4 = new TimedValue(4,1);
	TimedValue tv5 = new TimedValue(5,1);

	@Test
	public void testAll() throws Exception {
		NQueue nQueue = new NQueueImpl();

		assertFalse(nQueue.isFull());

		nQueue.offer(tv1);

		assertSame(tv1, nQueue.get(3));
		assertFalse(nQueue.isFull());

		nQueue.offer(tv2);

		assertSame(tv1, nQueue.get(2));
		assertSame(tv2, nQueue.get(3));
		assertFalse(nQueue.isFull());

		nQueue.offer(tv3);

		assertSame(tv1, nQueue.get(1));
		assertSame(tv2, nQueue.get(2));
		assertSame(tv3, nQueue.get(3));
		assertFalse(nQueue.isFull());

		nQueue.offer(tv4);

		assertSame(tv1, nQueue.get(0));
		assertSame(tv2, nQueue.get(1));
		assertSame(tv3, nQueue.get(2));
		assertSame(tv4, nQueue.get(3));
		assertTrue(nQueue.isFull());

		nQueue.offer(tv5);

		assertSame(tv2, nQueue.get(0));
		assertSame(tv3, nQueue.get(1));
		assertSame(tv4, nQueue.get(2));
		assertSame(tv5, nQueue.get(3));
		assertTrue(nQueue.isFull());
	}

	@Test
	public void testDuplicates() throws Exception {
		NQueue nQueue = new NQueueImpl();

		nQueue.offer(tv1);
		nQueue.offer(tv2);
		nQueue.offer(tv3);
		nQueue.offer(tv3dup);

		assertSame(tv1, nQueue.get(1));
		assertSame(tv2, nQueue.get(2));
		assertSame(tv3, nQueue.get(3));
		assertFalse(nQueue.isFull());
	}

	@Test(expected = AssertionError.class)
	public void testOutOfOrder() throws Exception {
		NQueue nQueue = new NQueueImpl();

		nQueue.offer(tv2);
		nQueue.offer(tv1);
	}


}
