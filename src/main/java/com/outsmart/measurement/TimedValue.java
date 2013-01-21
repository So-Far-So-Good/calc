package com.outsmart.measurement;

/**
 * @author Vadim Bobrov
 */
public class TimedValue implements Comparable<TimedValue> {

	private final long timestamp;
	private final double value;

	public TimedValue(long timestamp, double value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	public long timestamp() {
		return timestamp;
	}

	public double value() {
		return value;
	}

	@Override
	public String toString() {
		return "ts: " + timestamp + " value: " + value;
	}


	@Override
	public int compareTo(TimedValue o) {
		return this.timestamp - o.timestamp < 0 ? -1 : (this.timestamp - o.timestamp > 0 ? 1 : 0 );
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TimedValue that = (TimedValue) o;

		if (timestamp != that.timestamp) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return (int) (timestamp ^ (timestamp >>> 32));
	}

}
