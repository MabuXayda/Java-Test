package com.fpt.ftel.paytv.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.core.utils.NumberUtils;

public class StatisticUtils {

	public static boolean willProcessRealTimePlaying(String customerId, DateTime received_at, Double realTimePlaying,
			Map<String, DateTime> mapCheckValidRTP) {
		boolean willProcess = false;
		if (realTimePlaying != null) {
			if (!mapCheckValidRTP.containsKey(customerId)) {
				mapCheckValidRTP.put(customerId, received_at);
				willProcess = true;
			} else if (realTimePlaying < new Duration(mapCheckValidRTP.get(customerId), received_at)
					.getStandardSeconds() + Integer.parseInt(CommonConfig.get(PayTVConfig.DELAY_ALLOW_RTP))) {
				mapCheckValidRTP.put(customerId, received_at);
				willProcess = true;
			}
		}
		return willProcess;
	}

	public static boolean willProcessSessionMainMenu(String customerId, String unparseSMM, DateTime sessionMainMenu,
			DateTime received_at, Map<String, Set<String>> mapCheckDupSMM, Map<String, DateTime> mapCheckValidSMM) {
		boolean willProcess = false;
		Set<String> setCheckDupSMM = new HashSet<>();
		if (!mapCheckDupSMM.containsKey(customerId)) {
			setCheckDupSMM.add(unparseSMM);
			mapCheckDupSMM.put(customerId, setCheckDupSMM);
			willProcess = true;
		} else {
			setCheckDupSMM = mapCheckDupSMM.get(customerId);
			if (!setCheckDupSMM.contains(unparseSMM)) {
				setCheckDupSMM.add(unparseSMM);
				mapCheckDupSMM.put(customerId, setCheckDupSMM);
				if (!mapCheckValidSMM.containsKey(customerId)) {
					mapCheckValidSMM.put(customerId, received_at);
					willProcess = true;
				} else if (new Duration(mapCheckValidSMM.get(customerId), sessionMainMenu)
						.getStandardSeconds() > (-60)) {
					mapCheckValidSMM.put(customerId, received_at);
					willProcess = true;
				}
			}
		}
		return willProcess;
	}

	public static void printDetailApp(PrintWriter pr,
			Map<String, Map<String, Map<Integer, Integer>>> mapUserAppDetailHourly) {
		pr.print("CustomerId,AppName");
		for (int i = 0; i < 24; i++) {
			pr.print("," + i);
		}
		pr.println();
		for (String customerId : mapUserAppDetailHourly.keySet()) {
			Map<String, Map<Integer, Integer>> mapDetail = mapUserAppDetailHourly.get(customerId);
			for (String app : mapDetail.keySet()) {
				Map<Integer, Integer> mapUsage = mapDetail.get(app);
				pr.print(customerId + "," + app);
				for (int i = 0; i < 24; i++) {
					pr.print("," + (mapUsage.get(i) == null ? 0 : mapUsage.get(i)));
				}
				pr.println();
			}
		}
		pr.close();
	}

	public static void printDays48(PrintWriter pr, Map<String, Map<Integer, Integer>> totalMapDays) {
		pr.print("CustomerId");
		for (int i = 0; i <= 47; i++) {
			pr.print("," + i);
		}
		pr.println();
		for (String customerId : totalMapDays.keySet()) {
			pr.print(customerId);
			Map<Integer, Integer> mapDays = totalMapDays.get(customerId);
			for (int i = 0; i <= 47; i++) {
				Integer value = mapDays.get(i);
				if (value == null) {
					value = 0;
				}
				pr.print("," + value);
			}
			pr.println();
		}
		pr.close();
	}

	public static void printDays(PrintWriter pr, Map<String, Map<Integer, Integer>> totalMapDays) {
		pr.print("CustomerId");
		for (int i = 0; i <= 27; i++) {
			pr.print("," + i);
		}
		pr.println();
		for (String customerId : totalMapDays.keySet()) {
			pr.print(customerId);
			Map<Integer, Integer> mapDays = totalMapDays.get(customerId);
			for (int i = 0; i <= 27; i++) {
				Integer value = mapDays.get(i);
				if (value == null) {
					value = 0;
				}
				pr.print("," + value);
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateDays(Map<Integer, Integer> mapDays, int day, int seconds, DateTime received_at, int max) {
		DateTime dateTimeAtStartOfDate = received_at.withTimeAtStartOfDay();
		int availableSeconds = (int) new Duration(dateTimeAtStartOfDate, received_at).getStandardSeconds();
		int validSeconds = seconds;
		int remainSeconds = 0;
		int previousDay = 0;
		if (seconds > availableSeconds) {
			validSeconds = availableSeconds;
			remainSeconds = seconds - validSeconds;
			previousDay = day + 1;
		}
		if (mapDays.containsKey(day)) {
			mapDays.put(day, mapDays.get(day) + validSeconds);
		} else {
			mapDays.put(day, validSeconds);
		}
		if (remainSeconds > 0 && previousDay <= max) {
			if (mapDays.containsKey(previousDay)) {
				mapDays.put(previousDay, mapDays.get(previousDay) + remainSeconds);
			} else {
				mapDays.put(previousDay, remainSeconds);
			}
		}
	}

	public static void printReturnUse(PrintWriter pr, Map<String, Map<Integer, Integer>> totalMapReturnUse) {
		pr.println("CustomerId,ReuseCount,ReuseSum,ReuseAvg,ReuseMax");
		for (String customerId : totalMapReturnUse.keySet()) {
			Map<Integer, Integer> mapUse = totalMapReturnUse.get(customerId);
			int count = 0;
			int sum = 0;
			int max = 0;
			int start = 0;
			double avg = 0;

			for (int i = 1; i <= 27; i++) {

				if (i < 27 && (mapUse.get(i) == null ? 0 : mapUse.get(i)) == 1) {
					count += 1;
					sum = sum + (i - start);
					max = Math.max(max, i - start);
					start = i;
				} else if (i == 27) {
					sum = sum + (i - start);
					max = Math.max(max, i - start);
				}
			}
			if (count == 0) {
				avg = sum;
			} else {
				avg = sum / ((double) count + 1);
			}
			pr.println(customerId + "," + count + "," + sum + "," + NumberUtils.FORMAT_DOUBLE.format(avg) + "," + max);
		}
		pr.close();
	}

	public static Map<String, Map<String, Double>> calculateReturnUse(Map<String, Map<Integer, Integer>> vectorDays) {
		Map<String, Map<String, Double>> result = new HashMap<>();
		for (String customerId : vectorDays.keySet()) {
			Map<Integer, Integer> mapUse = vectorDays.get(customerId);
			Map<String, Double> mapReuseInfo = new HashMap<>();
			int count = 0;
			int sum = 0;
			int max = 0;
			int start = 0;
			double avg = 0;

			for (int i = 1; i <= 27; i++) {

				if (i < 27 && (mapUse.get(i) == null ? 0 : mapUse.get(i)) >= 1) {
					count += 1;
					sum = sum + (i - start);
					max = Math.max(max, i - start);
					start = i;
				} else if (i == 27) {
					sum = sum + (i - start);
					max = Math.max(max, i - start);
				}
			}
			if (count == 0) {
				avg = sum;
			} else {
				avg = sum / ((double) count + 1);
			}
			mapReuseInfo.put("ReuseCount", (double) count);
			mapReuseInfo.put("ReuseAvg", avg);
			mapReuseInfo.put("ReuseMax", (double) max);
			result.put(customerId, mapReuseInfo);
		}
		return result;
	}

	public static void printLogIdCount(PrintWriter pr, Set<String> setItem,
			Map<String, Map<String, Integer>> totalMapLogIdCount) {
		List<String> listItem = new ArrayList<>(setItem);
		pr.print("CustomerId");
		for (String item : listItem) {
			pr.print("," + item);
		}
		pr.println();
		for (String customerId : totalMapLogIdCount.keySet()) {
			pr.print(customerId);
			for (String item : listItem) {
				Integer value = totalMapLogIdCount.get(customerId).get(item);
				if (value == null) {
					value = 0;
				}
				pr.print("," + value);
			}
			pr.println();
		}

		pr.close();
	}

	public static void updateLogIdCount(Map<String, Integer> mapLogId, String item) {
		if (mapLogId.containsKey(item)) {
			mapLogId.put(item, mapLogId.get(item) + 1);
		} else {
			mapLogId.put(item, 1);
		}
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
				Integer value = totalMapApp.get(customerId).get(appName);
				if (value == null) {
					value = 0;
				}
				pr.print("," + value);
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateApp(Map<String, Integer> mapApp, String appName, int seconds) {
		if (mapApp.containsKey(appName)) {
			mapApp.put(appName, mapApp.get(appName) + seconds);
		} else {
			mapApp.put(appName, seconds);
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
				Integer value = mapHourly.get(i);
				if (value == null) {
					value = 0;
				}
				pr.print("," + value);
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateHourly(Map<Integer, Integer> mapHourly, DateTime stopTime, int seconds) {
		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > seconds) {
			currentTime = seconds;
		}
		int currentHour = stopTime.getHourOfDay();
		MapUtils.updateMapIntegerInteger(mapHourly, currentHour, currentTime);

		int beforeTime = seconds - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentHour = stopTime.getHourOfDay();
				MapUtils.updateMapIntegerInteger(mapHourly, currentHour, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentHour = stopTime.getHourOfDay();
			MapUtils.updateMapIntegerInteger(mapHourly, currentHour, finalTime);
		}
	}

	public static void printDaily(PrintWriter pr, Map<String, Map<String, Integer>> totalMapDaily) throws IOException {
		pr.print("CustomerId");
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			pr.print("," + day);
		}
		pr.println();
		for (String customerId : totalMapDaily.keySet()) {
			pr.print(customerId);
			Map<String, Integer> mapDaily = totalMapDaily.get(customerId);
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				Integer value = mapDaily.get(day);
				if (mapDaily.get(day) == null) {
					value = 0;
				}
				pr.print("," + value);
			}
			pr.println();
		}
		pr.close();
	}

	public static void updateDaily(Map<String, Integer> mapDaily, DateTime stopTime, int seconds) {
		int currentTime = stopTime.getHourOfDay() * 60 + stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		if (currentTime > seconds) {
			currentTime = seconds;
		}
		String currentDay = DateTimeUtils.getDayOfWeek(stopTime);
		MapUtils.updateMapStringInteger(mapDaily, currentDay, currentTime);

		int beforeTime = seconds - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);

			int times = beforeTime / (24 * 3600);
			int finalTime = beforeTime % (24 * 3600);

			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(24 * 3600);
				currentDay = DateTimeUtils.getDayOfWeek(stopTime);
				MapUtils.updateMapStringInteger(mapDaily, currentDay, 24 * 3600);
			}

			stopTime = stopTime.minus(finalTime);
			currentDay = DateTimeUtils.getDayOfWeek(stopTime);
			MapUtils.updateMapStringInteger(mapDaily, currentDay, finalTime);
		}
	}

}
