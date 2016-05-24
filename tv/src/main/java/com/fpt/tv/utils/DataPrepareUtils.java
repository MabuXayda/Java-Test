package com.fpt.tv.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

public class DataPrepareUtils {
	public static void printCountItem(PrintWriter pr, Set<String> setItem,
			Map<String, Map<String, Integer>> totalMapCountItem) {
		List<String> listItem = new ArrayList<>(setItem);
		pr.print("CustomerId");
		for (String item : listItem) {
			pr.print("," + item);
		}
		pr.println();
		for (String customerId : totalMapCountItem.keySet()) {
			pr.print(customerId);
			for (String item : listItem) {
				pr.print(" , " + totalMapCountItem.get(customerId).get(item));
			}
			pr.println();
		}
		pr.close();
	}

	public static void printWeek(PrintWriter pr, Map<String, Map<Integer, Integer>> totalMapWeek) {
		pr.println("CustomerId,week_1,week_2,week_3,week_4,week_5");
		for (String customerId : totalMapWeek.keySet()) {
			pr.print(customerId);
			for (int i = 1; i <= 5; i++) {
				pr.print("," + totalMapWeek.get(customerId).get(i));
			}
			pr.println();
		}
		pr.close();
	}

	public static void printApp(PrintWriter pr, Map<String, Map<String, Integer>> totalMapApp) throws IOException {
		pr.print("CustomerId");
		for (String appName : Utils.LIST_APP_NAME) {
			pr.print("," + appName);
		}
		pr.println();
		for (String customerId : totalMapApp.keySet()) {
			pr.print(customerId);
			for (String appName : Utils.LIST_APP_NAME) {
				pr.print("," + totalMapApp.get(customerId).get(appName));
			}
			pr.println();
		}
		pr.close();
	}

	public static void printAppTotal(PrintWriter pr, List<String> listAppName,
			Map<String, Map<String, Integer>> totalMapApp) throws IOException {
		pr.print("CustomerId");
		for (String appName : listAppName) {
			pr.print("," + appName);
		}
		pr.println();
		for (String customerId : totalMapApp.keySet()) {
			pr.print(customerId);
			for (String appName : listAppName) {
				pr.print("," + totalMapApp.get(customerId).get(appName));
			}
			pr.println();
		}
		pr.close();
	}

	public static void printHourly(PrintWriter prHourly, Map<String, Map<Integer, Integer>> totalMapHourly)
			throws IOException {
		Utils.printLabelHourly(prHourly);
		for (String customerId : totalMapHourly.keySet()) {
			Utils.printHourly(prHourly, customerId, totalMapHourly.get(customerId));
		}
		prHourly.close();
	}

	public static void printDaily(PrintWriter prDaily, Map<String, Map<String, Integer>> totalMapDaily)
			throws IOException {
		Utils.printLabelDaily(prDaily);
		for (String customerId : totalMapDaily.keySet()) {
			Utils.printDaily(prDaily, customerId, totalMapDaily.get(customerId));
		}
		prDaily.close();
	}

	public static void updateCountItem(Map<String, Integer> mapCountItem, String item) {
		if (mapCountItem.containsKey(item)) {
			mapCountItem.put(item, mapCountItem.get(item) + 1);
		} else {
			mapCountItem.put(item, 1);
		}
	}

	public static void updateWeek(Map<Integer, Integer> mapWeek, DateTime stopTime, int duration) {
		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		int currentWeek = Utils.getWeekOfMonth(stopTime.getDayOfMonth() + 1);
		addMapIntInt(mapWeek, currentWeek, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentWeek = Utils.getWeekOfMonth(stopTime.getDayOfMonth() + 1);
				addMapIntInt(mapWeek, currentWeek, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentWeek = Utils.getWeekOfMonth(stopTime.getDayOfMonth() + 1);
			addMapIntInt(mapWeek, currentWeek, finalTime);
		}
	}

	public static void updateApp(Map<String, Integer> mapApp, String appName, int duration) {
		if (mapApp.containsKey(appName)) {
			mapApp.put(appName, mapApp.get(appName) + duration);
		} else {
			mapApp.put(appName, duration);
		}
	}

	public static void updateHourlyDaily(Map<Integer, Integer> mapHourly, Map<String, Integer> mapDaily,
			DateTime stopTime, int duration) {

		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		int currentHour = stopTime.getHourOfDay();
		String currentDay = Utils.getDayOfWeek(stopTime);
		addMapIntInt(mapHourly, currentHour, currentTime);
		addMapStringInt(mapDaily, currentDay, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentHour = stopTime.getHourOfDay();
				currentDay = Utils.getDayOfWeek(stopTime);
				addMapIntInt(mapHourly, currentHour, 3600);
				addMapStringInt(mapDaily, currentDay, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentHour = stopTime.getHourOfDay();
			currentDay = Utils.getDayOfWeek(stopTime);
			addMapIntInt(mapHourly, currentHour, finalTime);
			addMapStringInt(mapDaily, currentDay, finalTime);
		}
	}

	public static void updateHourly(Map<Integer, Integer> mapHourly, DateTime stopTime, int duration) {
		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		int currentHour = stopTime.getHourOfDay();
		addMapIntInt(mapHourly, currentHour, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentHour = stopTime.getHourOfDay();
				addMapIntInt(mapHourly, currentHour, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentHour = stopTime.getHourOfDay();
			addMapIntInt(mapHourly, currentHour, finalTime);
		}
	}

	public static void updateDaily(Map<String, Integer> mapDaily, DateTime stopTime, int duration) {
		int currentTime = stopTime.getHourOfDay() * 60 + stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		String currentDay = Utils.getDayOfWeek(stopTime);
		addMapStringInt(mapDaily, currentDay, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);

			int times = beforeTime / (24 * 3600);
			int finalTime = beforeTime % (24 * 3600);

			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(24 * 3600);
				currentDay = Utils.getDayOfWeek(stopTime);
				addMapStringInt(mapDaily, currentDay, 24 * 3600);
			}

			stopTime = stopTime.minus(finalTime);
			currentDay = Utils.getDayOfWeek(stopTime);
			addMapStringInt(mapDaily, currentDay, finalTime);
		}
	}

	public static void addMapIntInt(Map<Integer, Integer> mapMain, Integer key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}

	public static void addMapStringInt(Map<String, Integer> mapMain, String key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}
}
