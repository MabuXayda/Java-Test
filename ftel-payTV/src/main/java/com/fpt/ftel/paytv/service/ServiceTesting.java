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
import com.fpt.ftel.paytv.object.raw.Fields;
import com.fpt.ftel.paytv.object.raw.Source;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;

public class ServiceTesting {
	public static void main(String[] args) throws IOException {

		String dateNow = "2016-09-09 01:01:00";
		String dateDaily = "2016-09-08 01:01:00";

		// createNow();
		processNow(dateNow);
		// createTable();
//		updateTable(dateDaily);
		// processProfile(dateProfile);

		// createProfile();
	}

	public static void createNow() {
		ServiceTableNow tableNowService = new ServiceTableNow();
		try {
			tableNowService.processTableNowCreateTable();
			System.out.println("============> DONE CREATE NOW");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void processNow(String dateString) {
		ServiceTableNow tableNowService = new ServiceTableNow();
		try {
			tableNowService.processTableNowReal(dateString);
			System.out.println("============> DONE PROCESS TABLE NOW");
		} catch (NumberFormatException | IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createTable() {
		ServiceDaily tableDailyService = new ServiceDaily();
		try {
			tableDailyService.processCreateTable();
			System.out.println("============> DONE CREATE DAILY");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void updateTable(String dateString) {
		ServiceDaily tableDailyService = new ServiceDaily();
		try {
			tableDailyService.processTableReal(dateString);
			System.out.println("============> DONE PROCESS TABLE DAILY");
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
						Source source = new Gson().fromJson(line, Source.class);
						Fields fields = source.getFields();
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
