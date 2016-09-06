package com.fpt.ftel.paytv.utils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class PayTVDBUtils {
	public static final String VECTOR_HOURLY_PREFIX = "h_";
	public static final String VECTOR_APP_PREFIX = "a_";
	public static final String VECTOR_DAILY_PREFIX = "d_";
	public static final String VECTOR_DAYS_PREFIX = "ds_";
	public static final String VECTOR_LOG_ID_PREFIX = "l_";

	public static final String MACHINE_LEARNING_7_SUBFIX = "_ml7";
	public static final String MACHINE_LEARNING_28_SUBFIX = "_ml28";

	public static void main(String[] args) {
		Map<String, Integer> map = new HashMap<>();
		map.put("h_01", 44545);
		map.put("h_02", 45);
		map.put("h_03", 23232);
		System.out.println(toJson(map));

		Map<String, Integer> resultMap = getVectorHourlyFromJson(toJson(map));
		for (String id : resultMap.keySet()) {
			System.out.print("|" + id + ":" + resultMap.get(id));
		}
	}

	@SuppressWarnings("rawtypes")
	public static String toJson(Map map) {
		return new Gson().toJson(map);
	}

	public static String getJsonVectorDays(Map<String, Integer> vectorDays) {
		Map<String, Integer> map = new HashMap<>();
		for (int i = 0; i < 28; i++) {
			String key = PayTVDBUtils.VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(i);
			map.put(key, vectorDays.get(key));
		}
		return toJson(map);
	}

	public static String getJsonVectorHourly(Map<String, Integer> vectorHourly) {
		Map<String, Integer> map = new HashMap<>();
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			map.put(key, vectorHourly.get(key));
		}
		return toJson(map);
	}

	public static String getJsonVectorApp(Map<String, Integer> vectorApp) {
		Map<String, Integer> map = new HashMap<>();
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
			map.put(key, vectorApp.get(key));
		}
		return toJson(map);
	}

	public static String getJsonVectorDaily(Map<String, Integer> vectorDaily) {
		Map<String, Integer> map = new HashMap<>();
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
			map.put(key, vectorDaily.get(key));
		}
		return toJson(map);
	}

	public static String generatedJsonEmptyVectorDays() {
		Map<String, Integer> map = new HashMap<>();
		for (int i = 0; i < 28; i++) {
			map.put(PayTVDBUtils.VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(i), 0);
		}
		return toJson(map);
	}

	public static String generatedJsonEmptyVectorHourly() {
		Map<String, Integer> map = new HashMap<>();
		for (int i = 0; i < 24; i++) {
			map.put(PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i), 0);
		}
		return toJson(map);
	}

	public static String generatedJsonEmptyVectorApp() {
		Map<String, Integer> map = new HashMap<>();
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			map.put(PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase(), 0);
		}
		return toJson(map);
	}

	public static String generatedJsonEmptyVectorDaily() {
		Map<String, Integer> map = new HashMap<>();
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			map.put(PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase(), 0);
		}
		return toJson(map);
	}

	public static Map<String, Integer> getVectorDaysFromJson(String json) {
		Type type = new TypeToken<Map<String, Integer>>() {
		}.getType();
		Map<String, Integer> map = new Gson().fromJson(json, type);
		Map<String, Integer> result = new HashMap<>();
		for (int i = 0; i < 28; i++) {
			String key = PayTVDBUtils.VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(i);
			result.put(key, map.get(key) == null ? 0 : map.get(key));
		}
		return result;
	}

	public static Map<String, Integer> getVectorHourlyFromJson(String json) {
		Type type = new TypeToken<Map<String, Integer>>() {
		}.getType();
		Map<String, Integer> map = new Gson().fromJson(json, type);
		Map<String, Integer> result = new HashMap<>();
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			result.put(key, map.get(key) == null ? 0 : map.get(key));
		}
		return result;
	}

	public static Map<String, Integer> getVectorAppFromJson(String json) {
		Type type = new TypeToken<Map<String, Integer>>() {
		}.getType();
		Map<String, Integer> map = new Gson().fromJson(json, type);
		Map<String, Integer> result = new HashMap<>();
		for (String app : PayTVUtils.SET_APP_NAME_RTP) {
			String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
			result.put(key, map.get(key) == null ? 0 : map.get(key));
		}
		return result;
	}

	public static Map<String, Integer> getVectorDailyFromJson(String json) {
		Type type = new TypeToken<Map<String, Integer>>() {
		}.getType();
		Map<String, Integer> map = new Gson().fromJson(json, type);
		Map<String, Integer> result = new HashMap<>();
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
			result.put(key, map.get(key) == null ? 0 : map.get(key));
		}
		return result;
	}

	public static Map<String, Integer> formatDBVectorHourly(Map<Integer, Integer> vectorHourly) {
		Map<String, Integer> temp = new HashMap<>();
		for (Integer key : vectorHourly.keySet()) {
			temp.put(VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(key), vectorHourly.get(key));
		}
		return temp;
	}

	public static Map<String, Integer> formatDBVectorDays(Map<Integer, Integer> vectorDays) {
		Map<String, Integer> temp = new HashMap<>();
		for (Integer key : vectorDays.keySet()) {
			temp.put(VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(key), vectorDays.get(key));
		}
		return temp;
	}

	public static Map<String, Integer> formatDBVectorDaily(Map<String, Integer> vectorDaily) {
		Map<String, Integer> temp = new HashMap<>();
		for (String key : vectorDaily.keySet()) {
			temp.put(VECTOR_DAILY_PREFIX + key.toLowerCase(), vectorDaily.get(key));
		}
		return temp;
	}

	public static Map<String, Integer> formatDBVectorApp(Map<String, Integer> vectorApp) {
		Map<String, Integer> temp = new HashMap<>();
		for (String key : vectorApp.keySet()) {
			temp.put(VECTOR_APP_PREFIX + key.toLowerCase(), vectorApp.get(key));
		}
		return temp;
	}

}
