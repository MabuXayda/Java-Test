package com.fpt.ftel.core.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class SupportData {

	
	public static void main(String[] args) {
		System.out.println(new Duration(Utils.DATE_TIME_FORMAT_WITH_HOUR.parseDateTime("2016-03-04 00:00:01"),
				Utils.DATE_TIME_FORMAT_WITH_HOUR.parseDateTime("2016-03-31 00:00:00")).getStandardDays());
	}

	public static Map<String, DateTime> getMapUserActiveDateCondition(DateTime dateCondition,
			Map<String, DateTime> mapUserActive) {
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		for (String customerId : mapUserActive.keySet()) {
			mapUserDateCondition.put(customerId, dateCondition);
		}
		return mapUserDateCondition;
	}

	public static Map<String, DateTime> getMapUserDateCondition(Map<String, DateTime> mapUserActive,
			Map<String, Map<String, DateTime>> mapUserChurn, String dateCondition) {
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		for (String customerId : mapUserActive.keySet()) {
			mapUserDateCondition.put(customerId, Utils.DATE_TIME_FORMAT_WITH_HOUR.parseDateTime(dateCondition));
		}
		
		for (String customerId : mapUserChurn.keySet()) {
			mapUserDateCondition.put(customerId, mapUserChurn.get(customerId).get("StopDate"));
		}
		return mapUserDateCondition;
	}

	public static Map<String, DateTime> getMapUserActive(String file) throws IOException {
		Map<String, DateTime> mapUserActive = new HashMap<>();
		int count = 0;
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 3) {
				if (Utils.isNumeric(arr[0])) {
					DateTime startDate = Utils.DATE_TIME_FORMAT.parseDateTime(arr[1]);
					if (startDate != null) {
						mapUserActive.put(arr[0], startDate);
						count++;
					}
				}
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Total user Active: " + count);
		return mapUserActive;
	}

	public static Map<String, Map<String, DateTime>> getMapUserChurn(String file) throws IOException {
		Map<String, Map<String, DateTime>> mapUserChurn = new HashMap<>();
		int count = 0;
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 4) {
				if (Utils.isNumeric(arr[0])) {
					DateTime startDate = Utils.DATE_TIME_FORMAT.parseDateTime(arr[1]);
					DateTime stopDate = Utils.DATE_TIME_FORMAT.parseDateTime(arr[2]);
					if (startDate != null && stopDate != null) {
						Map<String, DateTime> mapDate = new HashMap<>();
						mapDate.put("StartDate", startDate);
						mapDate.put("StopDate", stopDate);
						mapUserChurn.put(arr[0], mapDate);
						count++;
					}
				}
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Total user Churn: " + count);
		return mapUserChurn;
	}

}
