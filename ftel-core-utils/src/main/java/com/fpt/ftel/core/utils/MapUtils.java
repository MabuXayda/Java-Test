package com.fpt.ftel.core.utils;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {
	
	public static Map<String, Integer> plusMapStringInteger(Map<String, Integer> map1, Map<String, Integer> map2){
		Map<String, Integer> mapResult = new HashMap<>();
		for(String key : map1.keySet()){
			mapResult.put(key, map1.get(key) + map2.get(key));
		}
		return mapResult;
	}

	public static Map<String, Double> scaleMap(Map<String, Double> inputMap, Integer topValue) {
		Map<String, Double> outputMap = new HashMap<>();
		for (String key : inputMap.keySet()) {
			double value = inputMap.get(key) / topValue;
			outputMap.put(key, value);
		}
		return outputMap;
	}

	public static void updateMapIntegerInteger(Map<Integer, Integer> mainMap, Integer key, Integer value) {
		if (mainMap.containsKey(key)) {
			mainMap.put(key, mainMap.get(key) + value);
		} else {
			mainMap.put(key, value);
		}
	}

	public static void updateMapStringInteger(Map<String, Integer> mainMap, String key, Integer value) {
		if (mainMap.containsKey(key)) {
			mainMap.put(key, mainMap.get(key) + value);
		} else {
			mainMap.put(key, value);
		}
	}
}
