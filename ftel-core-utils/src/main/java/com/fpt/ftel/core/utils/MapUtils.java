package com.fpt.ftel.core.utils;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {
	
	public static Map<String, Integer> plusMapStringIntegerEasyLeft(Map<String, Integer> map1, Map<String, Integer> map2){
		Map<String, Integer> result = new HashMap<>();
		for(String key : map1.keySet()){
			Integer val1 = map1.get(key);
			Integer val2 = map2.get(key) == null ? 0 : map2.get(key);
			result.put(key, val1 + val2);
		}
		return result;
	}
	
	public static Map<String, Integer> plusMapStringIntegerHard(Map<String, Integer> map1, Map<String, Integer> map2){
		Map<String, Integer> result = new HashMap<>();
		for(String key : map1.keySet()){
			result.put(key, map1.get(key) + map2.get(key));
		}
		return result;
	}

	public static Map<String, Double> scaleMap(Map<String, Double> inputMap, Integer topValue) {
		Map<String, Double> result = new HashMap<>();
		for (String key : inputMap.keySet()) {
			double value = inputMap.get(key) / topValue;
			result.put(key, value);
		}
		return result;
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
