package com.fpt.tv.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Utils {
	public static final List<String> LIST_DAY_OF_WEEK = Arrays.asList("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");
	public static final Set<String> SET_APP_NAME_FULL = new HashSet<String>(Arrays.asList("HOME", "IPTV", "VOD",
			"SPORT", "CHILD", "RELAX", "SERVICE", "BHD", "FIMs", "FIRMWARE", "LOGIN", "PARENTAL CONTROL"));
	public static final Set<String> SET_APP_NAME_RTP = new HashSet<String>(
			Arrays.asList("IPTV", "VOD", "SPORT", "CHILD", "RELAX", "SERVICE", "BHD", "FIMs"));
	public static final Set<String> SET_LOG_ID_RTP = new HashSet<String>(
			Arrays.asList("42", "44", "451", "461", "415", "416", "52", "63", "681", "82", "133", "152"));
	public static final Set<String> SET_LOG_ID_TEMP = new HashSet<String>(Arrays.asList("41", "42", "52", "55"));
	public static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");
	public static final DateTimeFormatter DATE_TIME_FORMAT_WITH_HOUR = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	public static final String DATE_CONDITION_MONTH_3 = "2016-03-31 23:59:59";
	public static final String DATE_CONDITION_MONTH_4 = "2016-04-30 23:59:59";
	
	public static final Logger LOG_INFO = Logger.getLogger("InfoLog");
	public static final Logger LOG_ERROR = Logger.getLogger("ErrorLog");

	static {
		PropertyConfigurator.configure("./conf/log4j.properties");
	}

	public static void test() {
		List<String> listFile = new ArrayList<>();
		loadListFilePath(listFile, new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR)));
		for (String i : listFile) {
			System.out.println(i);
		}
	}

	public static void main(String[] args) throws NoSuchAlgorithmException {
		System.out.println("B046FCB79A4B:2016:04:01:06:44:40:965".substring(13));
//		String o = "/home/tunn/data/tv/log_parsed/t3/fbox-2016-03-20_parsed.csv";
//		System.out.println(o.substring(o.length() - 21, o.length() - 11));
//		System.out.println(isNumeric("11.9"));
	}
	
	public static String hashCode(String input) throws NoSuchAlgorithmException{
		MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(input.getBytes());
        byte byteData[] = md.digest();
        
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) {
			sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		}
		
		return sb.toString();
        
	}
	
	public static void createFolder(String folderPath){
		File theDir = new File(folderPath);
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}
	}

	public static void sortListFilePath(List<String> listFile) {
		Collections.sort(listFile, new Comparator<String>() {
			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");

			public DateTime getDateTime(String o) {
				return dtf.parseDateTime(o.split("/")[7].substring(5, 15));
			}

			public int compare(String o1, String o2) {
				try {
					return getDateTime(o1).compareTo(getDateTime(o2));
				} catch (Exception e) {
					throw e;
				}
			}
		});
	}
	
	public static void sortListFile(List<File> files) {
		Collections.sort(files, new Comparator<File>() {
			DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");

			public DateTime getDateTime(File file) {
				return dtf.parseDateTime(file.getAbsolutePath().split("/")[7].substring(5, 15));
			}

			public int compare(File file1, File file2) {
				try {
					return getDateTime(file1).compareTo(getDateTime(file2));
				} catch (Exception e) {
					throw e;
				}
			}
		});
	}

	public static void loadListFilePath(List<String> listFile, File file) {
		File[] subdir = file.listFiles();
		for (File f : subdir) {
			if (f.isFile()) {
				listFile.add(f.getAbsolutePath());
			}
			if (f.isDirectory()) {
				loadListFilePath(listFile, f);
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
