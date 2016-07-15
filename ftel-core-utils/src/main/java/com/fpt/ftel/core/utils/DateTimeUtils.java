package com.fpt.ftel.core.utils;

import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class DateTimeUtils {
	public static final List<String> LIST_DAY_OF_WEEK = Arrays.asList("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");

	public static int getDayDuration(DateTime dateBefore, DateTime dateAfter) {
		Duration duration = new Duration(dateBefore, dateAfter);
		return (int) Math.ceil(duration.getStandardHours() / 24.0) - 1;
	}

	public static int getWeekIndexFromDuration(int duration) {
		if (duration >= 21) {
			return 1;
		} else if (duration >= 14) {
			return 2;
		} else if (duration >= 7) {
			return 3;
		} else {
			return 4;
		}
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
