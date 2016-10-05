package com.fpt.ftel.paytv.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;

import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.paytv.object.RawLog;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;

public class Testing {
	public static void main(String[] args) throws IOException {
//		createApp();
//		processApp(PayTVUtils.FORMAT_DATE_TIME.print(time));
		
//		processNowFix("2016-09-18_00", "2016-09-18_23");
//		createDaily();
		processDailyFix("2016-09-18_00", "2016-09-18_23");
		
//		for (int i = 0; i < 24; i++) {
//			String date = PayTVUtils.FORMAT_DATE_TIME.print(time.plusHours(i));
//			processNow(date);
//		}
	}

	public static void createNow() {
		ServiceTableNow tableNowService = new ServiceTableNow();
		try {
			tableNowService.processCreateTable();
			System.out.println("============> DONE CREATE NOW");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void processNow(String dateString) {
		ServiceTableNow tableNowService = new ServiceTableNow();
		try {
			tableNowService.processTableReal(dateString);
			System.out.println("============> DONE PROCESS TABLE NOW");
		} catch (NumberFormatException | IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void processNowFix(String fromDate, String toDate) {
		ServiceTableNow tableNowService = new ServiceTableNow();
		try {
			tableNowService.processTableFix(fromDate, toDate);
			System.out.println("============> DONE PROCESS TABLE NOW FIX");
		} catch (NumberFormatException | IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createDaily() {
		ServiceDaily tableDailyService = new ServiceDaily();
		try {
			tableDailyService.processCreateTable();
			System.out.println("============> DONE CREATE DAILY");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void processDaily(String dateString) {
		ServiceDaily tableDailyService = new ServiceDaily();
		try {
			tableDailyService.processTableReal(dateString);
			System.out.println("============> DONE PROCESS TABLE DAILY");
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void processDailyFix(String fromDate, String toDate) {
		ServiceDaily tableDailyService = new ServiceDaily();
		try {
			tableDailyService.processTableFix(fromDate, toDate);
			System.out.println("============> DONE PROCESS TABLE DAILY FIX");
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void createApp(){
		ServiceTableApp service = new ServiceTableApp();
		try {
			service.processCreateTable();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void processApp(String dateString){
		ServiceTableApp service = new ServiceTableApp();
			try {
				service.processTableReal(dateString);
			} catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	public static void testParseLog(String folderPath, String output, String idCheck) throws IOException {
		List<String> listFilePath = FileUtils.getListFilePath(new File(folderPath));
		PrintWriter pr = new PrintWriter(new FileWriter(output, true));
		for (String filePath : listFilePath) {
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = br.readLine();
			while (line != null) {
				if (!line.isEmpty()) {
					try {
						RawLog.Source source = new Gson().fromJson(line, RawLog.Source.class);
						RawLog.Source.Fields fields = source.getFields();
						String customerId = fields.getCustomerId();
						if (customerId.equals(idCheck)) {
							pr.println(line);
						}
					} catch (Exception e) {
						PayTVUtils.LOG_ERROR.error("Error parse json: " + line);
					}
				}
				line = br.readLine();
			}
			br.close();
		}
		pr.close();
	}
}
