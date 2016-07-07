package com.fpt.ftel.core.utils;

import java.util.List;

public class ListUtils {
	
	public static double getMaxListDouble(List<Double> listDouble) {
		double max = listDouble.get(0);
		for (int i = 1; i < listDouble.size(); i++) {
			max = Math.max(max, listDouble.get(i));
		}
		return max;
	}

	public static double getAverageListDouble(List<Double> listDouble) {
		double sum = 0.0;
		for (int i = 0; i < listDouble.size(); i++) {
			sum += listDouble.get(i);
		}
		return sum / listDouble.size();
	}
}
