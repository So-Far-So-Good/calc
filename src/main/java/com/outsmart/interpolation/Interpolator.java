package com.outsmart.interpolation;


import com.outsmart.Measurement.TimedValue;

import java.util.List;

/**
 * @author Vadim Bobrov
 */
public interface Interpolator {
	List<TimedValue> offer(TimedValue tv);
}
