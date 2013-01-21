package com.outsmart.interpolation;


import com.outsmart.measurement.TimedValue;

import java.util.List;

/**
 * @author Vadim Bobrov
 */
public interface Interpolator {
	List<TimedValue> offer(TimedValue tv);
}
