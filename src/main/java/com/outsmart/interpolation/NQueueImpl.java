package com.outsmart.interpolation;


import com.outsmart.measurement.TimedValue;

/**
 * @author Vadim Bobrov
 */
public class NQueueImpl implements NQueue {

	private final TimedValue[] elems = new TimedValue[4];
	private int counter = 0;

	@Override
	public TimedValue get(int i) {
		return elems[i];
	}

	@Override
	public boolean isFull(){
		return counter == 4;
	}

	@Override
	public void offer(TimedValue tv) {
		// skip duplicates
		if(tv != null && !(elems[3] != null && tv.equals(elems[3]))) {
			// must be sorted
			assert (elems[3] == null || tv.compareTo(elems[3]) > 0);
			elems[0] = elems[1];
			elems[1] = elems[2];
			elems[2] = elems[3];
			elems[3] = tv;
			if(counter < 4) counter++;
		}
	}

}
