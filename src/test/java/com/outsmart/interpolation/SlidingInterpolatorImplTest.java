package com.outsmart.interpolation;

import com.outsmart.measurement.TimedValue;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Vadim Bobrov
 */
public class SlidingInterpolatorImplTest {

	private double delta = 1.0E-10;
	private SlidingInterpolatorImpl interpolator;

	@Before
	public void setUp() {
		interpolator = new SlidingInterpolatorImpl();
	}

	@Test
	public void testLinearInterpolate() throws Exception {
		assertEquals(interpolator.linearInterpolate(0.5, 0, 0, 1, 1), 0.5, delta);
		assertEquals(interpolator.linearInterpolate(0.5, 0, 1, 1, 1), 1, delta);
		assertEquals(interpolator.linearInterpolate(0.5, 0, 2, 1, 1), 1.5, delta);
	}

	@Test(expected = AssertionError.class)
	public void testLinearInterpolateFail() throws Exception {
		interpolator.linearInterpolate(0.5, 1, 0, 1, 1);
	}

	@Test
	public void testFindIntersection() throws Exception {
		assertEquals(interpolator.findIntersection(0, 1, 2, 2, 5, 5, 6, 7), new SlidingInterpolatorImpl.Point(4, 3));
	}

	@Test
	public void testFindIntersectionParallel() throws Exception {
		assertEquals(interpolator.findIntersection(0, 2, 2, 2, 5, 5, 6, 5), SlidingInterpolatorImpl.Point.parallel);
	}

	@Test
	public void testFindIntersectionOutside() throws Exception {
		assertEquals(interpolator.findIntersection(0, 1, 2, 2, 5, 7, 6, 5), SlidingInterpolatorImpl.Point.outside);
	}

	@Test(expected = AssertionError.class)
	public void testFindIntersectionOutOfOrder() throws Exception {
		interpolator.findIntersection(3, 1, 2, 2, 5, 5, 6, 7);
	}

	@Test
	public void testInterpolateEmpty() throws Exception {
		interpolator.offer(new TimedValue(1,1));
		interpolator.offer(new TimedValue(1,1));
		List<TimedValue> res = interpolator.offer(new TimedValue(1,1));
		assertEquals(res.size(), 0);
	}

	@Test
	public void testInterpolate() throws Exception {
		interpolator.offer(new TimedValue(119996,1));
		interpolator.offer(new TimedValue(119998,2));
		interpolator.offer(new TimedValue(120001,5));
		List<TimedValue> res = interpolator.offer(new TimedValue(120002,7));
		assertEquals(res.size(), 1);
		assertEquals(res.get(0).timestamp(), 120000);
		assertEquals(res.get(0).value(), 3, delta);
	}

	@Test
	public void testInterpolateLeftOfIntersection() throws Exception {
		interpolator.offer(new TimedValue(119997,1));
		interpolator.offer(new TimedValue(119999,2));
		interpolator.offer(new TimedValue(120002,5));
		List<TimedValue> res = interpolator.offer(new TimedValue(120003,7));
		assertEquals(res.size(), 1);
		assertEquals(res.get(0).timestamp(), 120000);
		assertEquals(res.get(0).value(), 2.5, delta);
	}

	@Test
	public void testInterpolateRightOfIntersection() throws Exception {
		interpolator.offer(new TimedValue(119995,5));
		interpolator.offer(new TimedValue(119997,3));
		interpolator.offer(new TimedValue(120001,5));
		List<TimedValue> res = interpolator.offer(new TimedValue(120002,6));
		assertEquals(res.size(), 1);
		assertEquals(res.get(0).timestamp(), 120000);
		assertEquals(res.get(0).value(), 4, delta);
	}

	@Test
	public void testInterpolateOnALower() throws Exception {
		interpolator.offer(new TimedValue(119997,1));
		interpolator.offer(new TimedValue(119999,2));
		interpolator.offer(new TimedValue(120001,6));
		List<TimedValue> res = interpolator.offer(new TimedValue(120002,8));
		assertEquals(res.size(), 1);
		assertEquals(res.get(0).timestamp(), 120000);
		assertEquals(res.get(0).value(), 4, delta);
	}

	@Test
	public void testInterpolateOnAnUpper() throws Exception {
		interpolator.offer(new TimedValue(119996,1));
		interpolator.offer(new TimedValue(119998,2));
		interpolator.offer(new TimedValue(120002,4));
		List<TimedValue> res = interpolator.offer(new TimedValue(120003,6));
		assertEquals(res.size(), 1);
		assertEquals(res.get(0).timestamp(), 120000);
		assertEquals(res.get(0).value(), 3, delta);
	}

	@Test
	public void testInterpolate2OrMoreMinuteBoundaries() throws Exception {
		interpolator.offer(new TimedValue(119995,5));
		interpolator.offer(new TimedValue(119997,3));
		interpolator.offer(new TimedValue(180001,60005));
		List<TimedValue> res = interpolator.offer(new TimedValue(180002,60006));
		assertEquals(res.size(), 2);

		assertEquals(res.get(0).timestamp(), 120000);
		assertEquals(res.get(0).value(), 4, delta);

		assertEquals(res.get(1).timestamp(), 180000);
		assertEquals(res.get(1).value(), 60004, delta);
	}

}
