package com.fpt.tv;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LogUtils {
	public static DateTime getDateFromSession(String session) {
		String stringDate = session.substring(13, 32);
		DateTimeFormatter dft = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		return dft.parseDateTime(stringDate);
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
}
