package com.fpt.tv.utils;

import java.util.Iterator;
import java.util.Map;

public class AnalysisUtils {
	private static final double LOG_NATURAL_OF_2 = Math.log(2.0);
	
	public static double getCosinSimilarity(Map<String, Double> p1, Map<String, Double> p2){
		double x = 0;
		double y1 = 0;
		double y2 = 0;
		Iterator<String> features = p1.keySet().iterator();
		while (features.hasNext()) {
			String feature = features.next();
			double value1 = p1.get(feature);
			if (p2.containsKey(feature)) {
				double value2 = p2.get(feature);
				x += value1 * value2;
				y1 += Math.pow(value1, 2);
				y2 += Math.pow(value2, 2);
			}
		}
		double y = Math.sqrt(y1) + Math.sqrt(y2);
		double result = 0;
		if (x != 0 && y != 0) {
			result = x / y;
		}
		return result;
	}
	
	public static double getKLDistance(Map<String, Double> distribution1, Map<String, Double> distribution2) {

		double xm = 0, ym = 0;
		Iterator<String> features = distribution1.keySet().iterator();
		while (features.hasNext()) {
			String feature = features.next();
			double value1 = distribution1.get(feature);
			if (distribution2.containsKey(feature)) {

				double value2 = distribution2.get(feature);
				if (value1 == 0 || value2 == 0) {
					continue;
				}
				System.out.println(value1 + " | " + value2);

				double means = 0.5 * (value1 + value2);

				double logValue1 = Math.log(value1);

				double logValue2 = Math.log(value2);

				double logMean = Math.log(means);

				xm += value2 * (logValue2 - logMean) / LOG_NATURAL_OF_2;
				ym += value1 * (logValue1 - logMean) / LOG_NATURAL_OF_2;
			}
		}

		double distance = 0.5 * (xm + ym);
		return distance;
	}
	
	 public static double klDivergence(Map<String, Double> p1,  Map<String, Double> p2) {
		double klDiv = 0.0;
		Iterator<String> features = p1.keySet().iterator();
		while (features.hasNext()) {
			String feature = features.next();
			double value1 = p1.get(feature);
			if (p2.containsKey(feature)) {
				double value2 = p2.get(feature);
				if (value1 == 0) {
					continue;
				}
				if (value2 == 0.0) {
					continue;
				}
				klDiv += value1 * Math.log(value1 / value2);
			}
		}
		return klDiv / LOG_NATURAL_OF_2; // moved this division out of the loop -DM
	    }
}
