package com.fpt.tv.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

public class AnalysisUtils {

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

	public static void updateCountItem(Map<String, Integer> mapCountItem, String item) {
		if (mapCountItem.containsKey(item)) {
			mapCountItem.put(item, mapCountItem.get(item) + 1);
		} else {
			mapCountItem.put(item, 1);
		}
	}

	public static void printApp(PrintWriter pr, Map<String, Map<String, Integer>> totalMapApp) throws IOException {
		pr.print("CustomerId");
		for (String appName : Utils.LIST_APP_NAME_RTP) {
			pr.print("," + appName);
		}
		pr.println();
		for (String customerId : totalMapApp.keySet()) {
			pr.print(customerId);
			for (String appName : Utils.LIST_APP_NAME_RTP) {
				pr.print("," + totalMapApp.get(customerId).get(appName));
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateApp(Map<String, Integer> mapApp, String appName, int duration) {
		if (mapApp.containsKey(appName)) {
			mapApp.put(appName, mapApp.get(appName) + duration);
		} else {
			mapApp.put(appName, duration);
		}
	}

	public static void printHourly(PrintWriter pr, Map<String, Map<Integer, Integer>> totalMapHourly)
			throws IOException {
		pr.print("CustomerId");
		for (int i = 0; i < 24; i++) {
			pr.print("," + i);
		}
		pr.println();
		for (String customerId : totalMapHourly.keySet()) {
			pr.print(customerId);
			Map<Integer, Integer> mapHourly = totalMapHourly.get(customerId);
			for (int i = 0; i < 24; i++) {
				pr.print("," + mapHourly.get(i));
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateHourly(Map<Integer, Integer> mapHourly, DateTime stopTime, int duration) {
		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		int currentHour = stopTime.getHourOfDay();
		Utils.addMapKeyIntValInt(mapHourly, currentHour, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentHour = stopTime.getHourOfDay();
				Utils.addMapKeyIntValInt(mapHourly, currentHour, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentHour = stopTime.getHourOfDay();
			Utils.addMapKeyIntValInt(mapHourly, currentHour, finalTime);
		}
	}

	public static void printDaily(PrintWriter pr, Map<String, Map<String, Integer>> totalMapDaily) throws IOException {
		pr.print("CustomerId");
		for (String day : Utils.LIST_DAY_OF_WEEK) {
			pr.print("," + day);
		}
		pr.println();
		for (String customerId : totalMapDaily.keySet()) {
			pr.print(customerId);
			Map<String, Integer> mapDaily = totalMapDaily.get(customerId);
			for (String day : Utils.LIST_DAY_OF_WEEK) {
				pr.print("," + mapDaily.get(day));
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateDaily(Map<String, Integer> mapDaily, DateTime stopTime, int duration) {
		int currentTime = stopTime.getHourOfDay() * 60 + stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		String currentDay = Utils.getDayOfWeek(stopTime);
		Utils.addMapKeyStrValInt(mapDaily, currentDay, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);

			int times = beforeTime / (24 * 3600);
			int finalTime = beforeTime % (24 * 3600);

			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(24 * 3600);
				currentDay = Utils.getDayOfWeek(stopTime);
				Utils.addMapKeyStrValInt(mapDaily, currentDay, 24 * 3600);
			}

			stopTime = stopTime.minus(finalTime);
			currentDay = Utils.getDayOfWeek(stopTime);
			Utils.addMapKeyStrValInt(mapDaily, currentDay, finalTime);
		}
	}

}
