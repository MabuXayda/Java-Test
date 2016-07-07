package com.fpt.ftel.raw;

import java.io.File;

import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.NumberUtils;

public class ParseLogService {
	
	public static void main(String[] args) {
		ParseLogService parseLogService = new ParseLogService();
		parseLogService.getProcessPath();
	}
	
	public String getProcessPath(){
		DateTime dateTime = new DateTime();
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = year + File.separator + NumberUtils.getTwoCharNumber(month) + File.separator + NumberUtils.getTwoCharNumber(day) + File.separator + NumberUtils.getTwoCharNumber(hour); 
		System.out.println(path);
		return dateTime.toString();
	}
}
