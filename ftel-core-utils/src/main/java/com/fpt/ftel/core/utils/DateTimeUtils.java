package com.fpt.ftel.core.utils;

import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;

public class DateTimeUtils {
	public static final List<String> LIST_DAY_OF_WEEK = Arrays.asList("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");
	
	public static Integer getWeekIndexFromDuration (long duration){
		if(duration >= 20){
			return 1;
		}else if (duration >= 13) {
			return 2;
		}else if (duration >= 6) {
			return 3;
		}else {
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
