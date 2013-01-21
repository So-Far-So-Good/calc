package com.outsmart.interpolation;

import com.outsmart.measurement.TimedValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Vadim Bobrov
 */
public class SlidingInterpolatorImpl implements Interpolator {

	private final NQueue nQueue = new NQueueImpl();

	@Override
	public List<TimedValue> offer(TimedValue tv) {
		nQueue.offer(tv);

		if(nQueue.isFull())
			return bilinear(nQueue.get(0), nQueue.get(1), nQueue.get(2), nQueue.get(3));
		else
			return Collections.emptyList();
	}

	protected List<TimedValue> bilinear(TimedValue tv1, TimedValue tv2, TimedValue tv3, TimedValue tv4) {
		return bilinear(tv1, tv2, tv3, tv4, 60000);
	}

	/**
	 * Calculates interpolation given four consecutive points
	 * including second and third point if fall on the boundary
	 *
	 * @param tv1 tv2 tv3 tv4	four consecutive points (must be in ascending order)
	 * @return					sequence of interpolations
	 */
	protected List<TimedValue> bilinear(TimedValue tv1, TimedValue tv2, TimedValue tv3, TimedValue tv4, int boundary) {
		// ensure strictly ascending
		//require(tv1 < tv2 && tv2 < tv3 && tv3 < tv4, "bilinear arguments out of order " + tv1.timestamp + " " + tv3.timestamp + " " + tv3.timestamp + " " + tv4.timestamp)

		List<TimedValue> output = new ArrayList<TimedValue>();

		if(!(tv1.compareTo(tv2) < 0 && tv2.compareTo(tv3) < 0 && tv3.compareTo(tv4) < 0 )) {
			//debug("skipping duplicate")
			return Collections.emptyList();
		}

		if (tv2.timestamp() % boundary == 0)
			output.add(tv2);


		// take 4 points and find intersection
		Point point = findIntersection(
				tv1.timestamp(), tv1.value(),
				tv2.timestamp(), tv2.value(),
				tv3.timestamp(), tv3.value(),
				tv4.timestamp(), tv4.value()
		);

		for (long innerBoundary = tv2.timestamp() - (tv2.timestamp() % boundary) + boundary; innerBoundary < tv3.timestamp(); innerBoundary += boundary) {

			double computedValue = 0;

			if (point.getX() > 0) {
				// intersection found
				// compare current minute boundary with intersection
				// compute interpolation using ether lower or upper 2 points
				if (point.getX() == innerBoundary)
					computedValue = point.getY();
				else if (point.getX() < innerBoundary)
					computedValue = linearInterpolate(innerBoundary, point.getX(), point.getY(), tv3.timestamp(), tv3.value());
				else
					computedValue = linearInterpolate(innerBoundary, tv2.timestamp(), tv2.value(), point.getX(), point.getY());

			} else {
				// no intersection - either parallel or outside period
				//use linear interpolation between two measurements
				computedValue = linearInterpolate(innerBoundary, tv2.timestamp(), tv2.value(), tv3.timestamp(), tv3.value());
			}

			// add to output
			output.add(new TimedValue(innerBoundary, computedValue));
		}

		if (tv3.timestamp() % boundary == 0)
			output.add(tv3);

		return output;
	}


	/**
	 * Find the X point of intersection of two lines
	 * even though X axis is longs (timestamps) they are converted to doubles to avoid division error
	 * the output is rounded to long again
	 *
	 * params: xy12 - 2 points of the first line, xy34 - 2 points of the second line
	 *
	 * prereq: strictly in ascending order by X
	 * @return (0,0) if parallel, (-1,-1) if intersect outside [x2, x3], coordinates of intersection otherwise
	 */
	protected Point findIntersection(double x1, double y1, double x2, double y2, double x3, double y3, double x4, double y4) {
		// ensure strictly ascending
		assert (x1 < x2 && x2 < x3 && x3 < x4);

		double slope1 = (y2 - y1)/(x2 - x1);
		double slope2 = (y4 - y3)/(x4 - x3);

		// if parallel
		if (slope1 == slope2)
			return Point.parallel;

		long x = Math.round((slope1 * x1 - y1 - slope2 * x3 + y3) / (slope1 - slope2));
		double y = (slope1 * slope2 * (x1 - x3) + slope1 * y3 - slope2 * y1) / (slope1 - slope2);

		if (x >= x2 && x <= x3)
			return new Point(x, y);
		else
			return Point.outside;
	}

	/**
	 * Linear interpolation (or extrapolation) at point x on a line with x1y1, x2y2
	 * @param x interpolation point converted to double for math
	 * @return interpolation value
	 */
	protected double linearInterpolate(double x, double x1, double y1, double x2, double y2) {
		assert (x1 != x2);
		return (x - x1) * (y2 - y1)/(x2 - x1) + y1;
	}

	static protected class Point {
		private final long x;
		private final double y;

		static public final Point parallel = new Point(0,0);
		static public final Point outside = new Point(-1, -1);

		public Point(long x, double y) {
			this.x = x;
			this.y = y;
		}

		public long getX() {
			return x;
		}

		public double getY() {
			return y;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Point point = (Point) o;

			if (x != point.x) return false;
			if (Double.compare(point.y, y) != 0) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result;
			long temp;
			result = (int) (x ^ (x >>> 32));
			temp = y != +0.0d ? Double.doubleToLongBits(y) : 0L;
			result = 31 * result + (int) (temp ^ (temp >>> 32));
			return result;
		}
	}

}
