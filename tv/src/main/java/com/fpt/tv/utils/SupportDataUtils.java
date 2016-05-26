package com.fpt.tv.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class SupportDataUtils {
	private final static String DIR = "/home/tunn/data/tv/support/using/";
	private final static String USER_NGHIEP_VU = DIR + "boxNghiepVu.csv";
	private final static String USER_DEMO = DIR + "boxDemo.csv";
	private final static String USER_ACTIVE = DIR + "activeUser.csv";
	private final static String USER_HUY = DIR + "churnUser.csv";
	private final static String USER_PACKAGE = DIR + "goidichvuUser.csv";

	public static Map<String, DateTime> getMapUserDateCondition(String stringDate, Set<String> setUserActive,
			Map<String, Map<String, DateTime>> mapUserHuy) {
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		DateTime dateCondition = DateTimeFormat.forPattern("dd/MM/yyyy").parseDateTime(stringDate);
		for (String customerId : setUserActive) {
			mapUserDateCondition.put(customerId, dateCondition);
		}
		for (String customerId : mapUserHuy.keySet()) {
			DateTime startDate = mapUserHuy.get(customerId).get("start");
			DateTime stopDate = mapUserHuy.get(customerId).get("stop");
			int daysActive = (int) new Duration(startDate, stopDate).getStandardDays();
			if (daysActive >= 28) {
				mapUserDateCondition.put(customerId, stopDate);
			}
		}
		return mapUserDateCondition;
	}

	public static Set<String> loadSetUserNghiepVu() throws IOException {
		Set<String> setUserNghiepVu = new HashSet<>();
		BufferedReader br = new BufferedReader(new FileReader(USER_NGHIEP_VU));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 3) {
				if (!arr[0].isEmpty() && arr[0] != null) {
					setUserNghiepVu.add(arr[0]);
				} else {
					System.out.println("User Nghiep Vu Error: " + line);
				}
			} else {
				System.out.println("User Nghiep Vu Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		return setUserNghiepVu;
	}

	public static Set<String> loadSetUserDemo() throws IOException {
		Set<String> setUserDemo = new HashSet<>();
		BufferedReader br = new BufferedReader(new FileReader(USER_DEMO));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 5) {
				if (!arr[0].isEmpty() && arr[0] != null) {
					setUserDemo.add(arr[0]);
				} else {
					System.out.println("User Demo Error: " + line);
				}
			} else {
				System.out.println("User Demo Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		return setUserDemo;
	}

	public static Set<String> loadSetUserActive(Map<String, Map<String, DateTime>> mapUserHuy) throws IOException {
		Set<String> setUserActive = new HashSet<>();
		BufferedReader br = new BufferedReader(new FileReader(USER_ACTIVE));
		String line = br.readLine();
		line = br.readLine();
		int count = 0;
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 3) {
				if (!arr[0].isEmpty() && arr[0] != null) {
					if (!mapUserHuy.containsKey(arr[0])) {
						setUserActive.add(arr[0]);
					} else {
						count++;
					}
				} else {
					System.out.println("User Active Error: " + line);
				}
			} else {
				System.out.println("User Active Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Total User Huy in Active: " + count);
		return setUserActive;
	}

	public static Map<String, Map<String, DateTime>> loadMapUserHuy() throws IOException {
		Map<String, Map<String, DateTime>> mapUserHuy = new HashMap<>();
		DateTimeFormatter dtf = DateTimeFormat.forPattern("dd/MM/yyyy");
		BufferedReader br = new BufferedReader(new FileReader(USER_HUY));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 5) {
				String customerId = arr[0];
				DateTime startDate = dtf.parseDateTime(arr[3]);
				DateTime stopDate = dtf.parseDateTime(arr[4]);
				if (customerId != null && !customerId.isEmpty()) {
					Map<String, DateTime> mapStartStopDate = new HashMap<>();
					mapStartStopDate.put("start", startDate);
					mapStartStopDate.put("stop", stopDate);
					mapUserHuy.put(customerId, mapStartStopDate);
				} else {
					System.out.println("User Huy Error: " + line);
				}
			} else {
				System.out.println("User Huy Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		return mapUserHuy;
	}

	public static Map<String, String> loadMapUserPackage() throws IOException {
		Map<String, String> mapUserPackage = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(USER_PACKAGE));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 3) {
				if (arr[0] != null && !arr[0].isEmpty()) {
					mapUserPackage.put(arr[0], arr[1]);
				} else {
					System.out.println("User Package Error: " + line);
				}
			} else {
				System.out.println("User Package Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		return mapUserPackage;
	}
}
