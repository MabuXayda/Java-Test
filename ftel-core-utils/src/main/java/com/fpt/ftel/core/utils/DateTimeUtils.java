package com.fpt.ftel.core.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateTimeUtils {
	public static final List<String> LIST_DAY_OF_WEEK = Arrays.asList("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");

	public static List<String> getListDateInMonth(LocalDate month) {
		DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd");
		List<String> result = new ArrayList<>();
		LocalDate firstDate = month.withDayOfMonth(1);
		LocalDate nextMonthFirstDate = firstDate.plusMonths(1);
		while (firstDate.isBefore(nextMonthFirstDate)) {
			result.add(df.print(firstDate));
			firstDate = firstDate.plusDays(1);
		}
		return result;
	}

	public static int getDayDuration(DateTime dateBefore, DateTime dateAfter) {
		Duration duration = new Duration(dateBefore.withTimeAtStartOfDay(), dateAfter.withTimeAtStartOfDay());
		return (int) duration.getStandardDays();
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

	public static int compareToHour(DateTime d1, DateTime d2) {
		if (DateTimeComparator.getDateOnlyInstance().compare(d1, d2) == 0) {
			if (d1.getHourOfDay() < d2.getHourOfDay()) {
				return -1;
			} else if (d1.getHourOfDay() > d2.getHourOfDay()) {
				return 1;
			} else {
				return 0;
			}
		}
		return DateTimeComparator.getDateOnlyInstance().compare(d1, d2);
	}

	public static int compareToDate(DateTime d1, DateTime d2) {
		return DateTimeComparator.getDateOnlyInstance().compare(d1, d2);
	}
}
