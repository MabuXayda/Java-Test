package com.fpt.ftel.core.utils;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {
	
	public static Map<String, Double> scaleMap(Map<String, Double> inputMap, Integer topValue) {
		Map<String, Double> outputMap = new HashMap<>();
		for (String key : inputMap.keySet()) {
			double value = inputMap.get(key) / topValue;
			outputMap.put(key, value);
		}
		return outputMap;
	}

	public static void updateMapIntegerInteger(Map<Integer, Integer> mapMain, Integer key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}

	public static void updateMapStringInteger(Map<String, Integer> mapMain, String key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}
}
