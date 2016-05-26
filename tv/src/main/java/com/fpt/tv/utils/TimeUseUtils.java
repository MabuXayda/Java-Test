package com.fpt.tv.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

public class TimeUseUtils {

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

	public static void printHourly(PrintWriter prHourly, Map<String, Map<Integer, Integer>> totalMapHourly)
			throws IOException {
		printLabelHourly(prHourly);
		for (String customerId : totalMapHourly.keySet()) {
			printUserHourly(prHourly, customerId, totalMapHourly.get(customerId));
		}
		prHourly.close();
	}

	public static void printDaily(PrintWriter prDaily, Map<String, Map<String, Integer>> totalMapDaily)
			throws IOException {
		printLabelDaily(prDaily);
		for (String customerId : totalMapDaily.keySet()) {
			printUserDaily(prDaily, customerId, totalMapDaily.get(customerId));
		}
		prDaily.close();
	}

	public static void printUserHourly(PrintWriter pr, String customerId, Map<Integer, Integer> mapHourly) {
		pr.print(customerId);
		for (int i = 0; i < 24; i++) {
			pr.print("," + mapHourly.get(i));
		}
		pr.println();
	}

	public static void printUserDaily(PrintWriter pr, String customerId, Map<String, Integer> mapDaily) {
		pr.print(customerId);
		for (String day : Utils.LIST_DAY_OF_WEEK) {
			pr.print("," + mapDaily.get(day));
		}
		pr.println();
	}

	public static void printLabelHourly(PrintWriter pr) {
		pr.print("CustomerId");
		for (int i = 0; i < 24; i++) {
			pr.print("," + i);
		}
		pr.println();
	}

	public static void printLabelDaily(PrintWriter pr) {
		pr.print("CustomerId");
		for (String day : Utils.LIST_DAY_OF_WEEK) {
			pr.print("," + day);
		}
		pr.println();
	}

	public static void updateWeek(Map<Integer, Integer> mapWeek, DateTime stopTime, int duration) {
		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > duration) {
			currentTime = duration;
		}
		int currentWeek = Utils.getWeekOfMonth(stopTime.getDayOfMonth() + 1);
		Utils.addMapKeyIntValInt(mapWeek, currentWeek, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentWeek = Utils.getWeekOfMonth(stopTime.getDayOfMonth() + 1);
				Utils.addMapKeyIntValInt(mapWeek, currentWeek, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentWeek = Utils.getWeekOfMonth(stopTime.getDayOfMonth() + 1);
			Utils.addMapKeyIntValInt(mapWeek, currentWeek, finalTime);
		}
	}

	public static void updateApp(Map<String, Integer> mapApp, String appName, int duration) {
		if (mapApp.containsKey(appName)) {
			mapApp.put(appName, mapApp.get(appName) + duration);
		} else {
			mapApp.put(appName, duration);
		}
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
