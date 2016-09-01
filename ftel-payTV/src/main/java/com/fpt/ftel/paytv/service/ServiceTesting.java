package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.SQLException;

public class ServiceTesting {
	public static void main(String[] args) {
		String dateNow = "2016-08-31 17:01:00";
		String dateDaily = "2016-09-01 04:01:00";
		String dateProfile = "2016-09-01 07:01:00";
		
//		processNow(dateNow);
//		processDaily(dateDaily);
		processProfile(dateProfile);
		
//		createProfile();
	}

	public static void createNow() {
		TableNowService tableNowService = new TableNowService();
		try {
			tableNowService.processTableNowCreateTable();
			System.out.println("============> DONE CREATE NOW");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void processNow(String dateString) {
		TableNowService tableNowService = new TableNowService();
		try {
			tableNowService.processTableNowReal(dateString);
			System.out.println("============> DONE PROCESS TABLE NOW");
		} catch (NumberFormatException | IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createDaily() {
		TableDailyService tableDailyService = new TableDailyService();
		try {
			tableDailyService.processTableDailyCreateTable();
			System.out.println("============> DONE CREATE DAILY");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void processDaily(String dateString) {
		TableDailyService tableDailyService = new TableDailyService();
		try {
			tableDailyService.processTableDailyReal(dateString);
			System.out.println("============> DONE PROCESS TABLE DAILY");
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createProfile() {
		TableProfileService tableProfileService = new TableProfileService();
		try {
			tableProfileService.processTableProfileCreateTable();
			System.out.println("============> DONE CREATE PROFILE");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void processProfile(String dateString) {
		TableProfileService tableProfileService = new TableProfileService();
		try {
			tableProfileService.processTableProfileReal(dateString);
			System.out.println("============> DONE PROCESS TABLE PROFILE");
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
