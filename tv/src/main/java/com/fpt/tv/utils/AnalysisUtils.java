package com.fpt.tv.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class AnalysisUtils {

	public static boolean willProcessRealTimePlaying(String customerId, DateTime received_at, Double realTimePlaying,
			Map<String, DateTime> mapCheckValidRTP) {
		boolean willProcess = false;
		if (realTimePlaying != null) {
			if (!mapCheckValidRTP.containsKey(customerId)) {
				mapCheckValidRTP.put(customerId, received_at);
				willProcess = true;
			} else if (realTimePlaying < new Duration(mapCheckValidRTP.get(customerId), received_at)
					.getStandardSeconds()) {
				mapCheckValidRTP.put(customerId, received_at);
				willProcess = true;
			}
		}
		return willProcess;
	}

	public static boolean willProcessSessionMainMenu(String customerId, String unparseSMM, DateTime sessionMainMenu,
			DateTime received_at, Map<String, Set<String>> mapCheckDupSMM, Map<String, DateTime> mapCheckValidSMM) {
		boolean willProcess = false;
		Set<String> setCheckDupSMM = mapCheckDupSMM.get(customerId);
		if (!setCheckDupSMM.contains(unparseSMM)) {
			setCheckDupSMM.add(unparseSMM);
			mapCheckDupSMM.put(customerId, setCheckDupSMM);
			if (!mapCheckValidSMM.containsKey(customerId)) {
				mapCheckValidSMM.put(customerId, received_at);
				willProcess = true;
			} else if (new Duration(mapCheckValidSMM.get(customerId), sessionMainMenu).getStandardSeconds() > (-60)) {
				mapCheckValidSMM.put(customerId, received_at);
				willProcess = true;
			}
		}
		return willProcess;
	}

	public static void printReturnUse(PrintWriter pr, Map<String, DateTime> mapUserDateCondition, Map<String, Integer> mapRTPTotalCount,
			Map<String, Integer> mapReturnUseCount, Map<String, Integer> mapReturnUseSum,
			Map<String, Integer> mapReturnUseMax) {
		pr.println("CustomerId,TotalUse,ReuseCount,ReuseSum,ReuseAvg,ReuseMax");
		for (String customerId : mapUserDateCondition.keySet()) {
			if (!mapReturnUseCount.containsKey(customerId)) {
				pr.println(customerId + "," + mapRTPTotalCount.get(customerId) + ",null,null,null,null");
			} else if (mapReturnUseCount.get(customerId) == 0) {
				pr.println(customerId + "," + mapRTPTotalCount.get(customerId) + ",0,0,0,0");
			} else {
				pr.println(customerId + "," + mapRTPTotalCount.get(customerId) + "," + mapReturnUseCount.get(customerId)
						+ "," + mapReturnUseSum.get(customerId) + ","
						+ mapReturnUseSum.get(customerId) / mapReturnUseCount.get(customerId) + ","
						+ mapReturnUseMax.get(customerId));
			}
		}
		pr.close();
	}

	public static void updateReturnUse(String customerId, DateTime received_at, int seconds,
			Map<String, DateTime> mapReturnUsePoint, Map<String, Integer> mapReturnUseCount,
			Map<String, Integer> mapReturnUseSum, Map<String, Integer> mapReturnUseMax) {
		DateTime startTime = received_at.minusSeconds(seconds);
		if (!mapReturnUsePoint.containsKey(customerId)) {
			mapReturnUsePoint.put(customerId, startTime);
			mapReturnUseCount.put(customerId, 0);
			mapReturnUseSum.put(customerId, 0);
			mapReturnUseMax.put(customerId, 0);
		} else {
			int reuseTime = (int) new Duration(mapReturnUsePoint.get(customerId), startTime).getStandardHours();
			if (reuseTime >= 23) {
				mapReturnUseCount.put(customerId, mapReturnUseCount.get(customerId) + 1);
				mapReturnUseSum.put(customerId, mapReturnUseSum.get(customerId) + reuseTime);
				mapReturnUseMax.put(customerId, Math.max(mapReturnUseMax.get(customerId), reuseTime));
				mapReturnUsePoint.put(customerId, startTime);
			}
		}
	}

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

	public static void updateAppHourlyDaily(String customerId, DateTime stopTime, String appName, int seconds,
			Map<String, Map<String, Integer>> totalMapApp, Map<String, Map<Integer, Integer>> totalMapHourly,
			Map<String, Map<String, Integer>> totalMapDaily) {

		Map<Integer, Integer> mapHourly = totalMapHourly.get(customerId);
		AnalysisUtils.updateHourly(mapHourly, stopTime, seconds);
		totalMapHourly.put(customerId, mapHourly);

		Map<String, Integer> mapDaily = totalMapDaily.get(customerId);
		AnalysisUtils.updateDaily(mapDaily, stopTime, seconds);
		totalMapDaily.put(customerId, mapDaily);

		Map<String, Integer> mapApp = totalMapApp.get(customerId);
		AnalysisUtils.updateApp(mapApp, appName, seconds);
		totalMapApp.put(customerId, mapApp);
	}

	public static void printApp(PrintWriter pr, Map<String, Map<String, Integer>> totalMapApp, Set<String> setAppName)
			throws IOException {
		List<String> listAppName = new ArrayList<>(setAppName);
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
