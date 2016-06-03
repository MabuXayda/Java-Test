package com.fpt.tv.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Utils {
	public static final List<String> LIST_DAY_OF_WEEK = Arrays.asList("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");
	public static final List<String> LIST_APP_NAME = Arrays.asList("HOME", "IPTV", "VOD", "SPORT", "CHILD", "RELAX",
			"SERVICE", "BHD", "FIMs");
	public static final Logger LOG_INFO = Logger.getLogger("InfoLog");
	public static final Logger LOG_ERROR = Logger.getLogger("ErrorLog");

	static {
		PropertyConfigurator.configure("./conf/log4j.properties");
	}

	public static void test() {
		String zz = "B046FCAB70D1:2016:06:15:24:54:50:659";
		DateTime day = parseSessionMainMenu(zz);
		System.out.println(day.getHourOfDay());
	}

	public static void main(String[] args) {
		// test();
		System.out.println(isNumeric(""));
	}

	public static void loadListFile(List<String> listFile, File file) {
		File[] subdir = file.listFiles();
		for (File f : subdir) {
			if (f.isFile()) {
				listFile.add(f.getAbsolutePath());
			}
			if (f.isDirectory()) {
				loadListFile(listFile, f);
			}
		}
	}
	
	public static PrintWriter getPrintWriter(String file) throws IOException {
		return new PrintWriter(new FileWriter(file));
	}

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}

	public static String getDayOfWeek(DateTime date) {
		String day = "";
		switch (date.getDayOfWeek()) {
		case 1:
			day = "Mon";
			break;
		case 2:
			day = "Tue";
			break;
		case 3:
			day = "Wed";
			break;
		case 4:
			day = "Thu";
			break;
		case 5:
			day = "Fri";
			break;
		case 6:
			day = "Sat";
			break;
		case 7:
			day = "Sun";
			break;
		}
		return day;
	}

	public static Double parseRealTimePlaying(String realTimePlaying) {
		Double time = null;
		try {
			time = Double.parseDouble(realTimePlaying);
			if (time.isNaN()) {
				time = null;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return time;
	}

	public static DateTime parseSessionMainMenu(String sessionMainMenu) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		if (sessionMainMenu.length() != 36) {
			return null;
		} else {
			DateTime dt = null;
			try {
				dt = dtf.parseDateTime(sessionMainMenu.substring(13, 32));
			} catch (Exception e) {
				// TODO: handle exception
			}
			return dt;
		}
	}

	public static DateTime parseBoxTime(String boxTime) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		if (boxTime.length() != 23) {
			return null;
		} else {
			DateTime dt = null;
			try {
				dt = dtf.parseDateTime(boxTime.substring(0, 19));

			} catch (Exception e) {
				// TODO: handle exception
			}
			return dt;
		}
	}

	public static DateTime parseReceived_at(String received_at) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
		if (received_at.length() != 25) {
			return null;
		} else {
			DateTime dt = null;
			try {
				dt = dtf.parseDateTime(received_at.substring(0, 19));
			} catch (Exception e) {
				// TODO: handle exception
			}
			return dt;
		}
	}

	public static void addMapKeyIntValInt(Map<Integer, Integer> mapMain, Integer key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}

	public static void addMapKeyStrValInt(Map<String, Integer> mapMain, String key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}

}
