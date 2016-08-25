package com.fpt.ftel.paytv.utils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class PayTVDBUtils {
	
	public static void main(String[] args) {
		Map<String, Map<String, Integer>> motherMap = new HashMap<>();
		Map<String, Integer> map = new HashMap<>();
		map.put("1", 44545);
		map.put("2", 45);
		motherMap.put("c1", map);
		map.put("3", 23232);
		motherMap.put("c2", map);
		System.out.println(toJson(motherMap));
		
		Map<String, Map<Integer,Integer>> resultMap = getVectorHourlyFromJson(toJson(motherMap));
		for(String id : resultMap.keySet()){
			Map<Integer, Integer> result = resultMap.get(id);
			System.out.print(id);
			for(Integer key : result.keySet()){
				System.out.print("|" + key + ":" + result.get(key));
			}
			System.out.println();
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static String toJson(Map map){
		return new Gson().toJson(map);
	}
	
	public static Map<String, Map<Integer, Integer>> getVectorHourlyFromJson(String json){
		Type type = new TypeToken<Map<String, Map<Integer, Integer>>>(){}.getType();
		Map<String, Map<Integer, Integer>> map = new Gson().fromJson(json, type);
		return map;
	}
	
}
