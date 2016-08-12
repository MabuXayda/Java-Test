package com.fpt.ftel.paytv.utils;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class PayTVUtils {
//	public static final int NUMBER_OF_FIELDS = 10;
	
	public static final Set<String> SET_APP_NAME_FULL = new HashSet<String>(Arrays.asList("HOME", "IPTV", "VOD",
			"SPORT", "CHILD", "RELAX", "SERVICE", "BHD", "FIMs", "FIRMWARE", "LOGIN", "PARENTAL CONTROL"));
	public static final Set<String> SET_APP_NAME_RTP = new HashSet<String>(
			Arrays.asList("IPTV", "VOD", "SPORT", "CHILD", "RELAX", "SERVICE", "BHD", "FIMs"));
	public static final List<String> LIST_APP_NAME_RTP = Arrays.asList("IPTV", "VOD", "SPORT", "CHILD", "RELAX",
			"SERVICE", "BHD", "FIMs");
	public static final Set<String> SET_LOG_ID_RTP = new HashSet<String>(
			Arrays.asList("42", "44", "451", "461", "415", "416", "52", "63", "681", "82", "133", "152"));

	public static final DateTimeFormatter FORMAT_DATE_TIME_SIMPLE = DateTimeFormat.forPattern("yyyy-MM-dd");
	public static final DateTimeFormatter FORMAT_DATE_TIME = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	public static final DateTimeFormatter FORMAT_DATE_TIME_HOUR = DateTimeFormat.forPattern("yyyy-MM-dd_HH");

	public static final Logger LOG_INFO = Logger.getLogger("InfoLog");
	public static final Logger LOG_ERROR = Logger.getLogger("ErrorLog");

//	static {
//		PropertyConfigurator.configure(CommonConfig.getInstance().get(CommonConfig.LOG4J_CONFIG_DIR));
//	}

	public static void main(String[] args) throws NoSuchAlgorithmException {
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

}
