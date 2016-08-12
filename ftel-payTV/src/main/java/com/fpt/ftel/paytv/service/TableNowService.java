package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.hdfs.HdfsIOSimple;
import com.fpt.ftel.paytv.db.PostgreSQLPayTV;
import com.fpt.ftel.paytv.statistic.UserUsage;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class TableNowService {
	private UserUsage userUsage;
	private HdfsIOSimple hdfsIOSimple;
	private PostgreSQLPayTV postgreSQLPayTV;
	private Connection connection;
	private CommonConfig cf;
	private static final String POSTGRESQL_PAYTV_TABLE_NOW = "now";

	public static void main(String[] args) throws IOException, SQLException {
		TableNowService tableNowService = new TableNowService();
		tableNowService.test();
//		if (args[0].equals("create table") && args.length == 1) {
//			System.out.println("Start table now create table job ..........");
//			tableNowService.processTableNowCreateTable();
//		} else if (args[0].equals("real") && args.length == 2) {
//			System.out.println("Start table now real job ..........");
//			tableNowService.processTableNowReal(args[1]);
//		}
//		System.out.println("DONE " + args[0] + " job");
	}
	
	public void test() throws IOException{
		for(int i = 0; i<24;i++){
			String dateString = "2016-08-10 " + NumberUtils.getTwoCharNumber(i) + ":10:00";
			processTableNowReal(dateString);
		}
	}

	public TableNowService() throws IOException {
		cf = CommonConfig.getInstance();
		PropertyConfigurator.configure(cf.get(CommonConfig.LOG4J_CONFIG_DIR) + "/log4j_TableNowService.properties");
		hdfsIOSimple = new HdfsIOSimple();
		userUsage = new UserUsage();
		postgreSQLPayTV = new PostgreSQLPayTV();
	}
	
	private void openPostgreSQLConnection(){
		connection = postgreSQLPayTV.getConnection(cf.get(CommonConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(cf.get(CommonConfig.POSTGRESQL_PAYTV_PORT)),
				cf.get(CommonConfig.POSTGRESQL_PAYTV_DATABASE), cf.get(CommonConfig.POSTGRESQL_PAYTV_USER),
				cf.get(CommonConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
	}
	
	private void closePostgreSQLConnection(){
		try {
			connection.close();
		} catch (SQLException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
		}
	}

	public void processTableNowReal(String dateString) throws IOException {
		openPostgreSQLConnection();
		List<String> listDateString = ParseLogService.getListParseMissing();
		List<String> listMissing = new ArrayList<>();
		DateTime currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(currentDateTime.minusHours(1)));
		for (String date : listDateString) {
			System.out.println(date);
			currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);
			if (currentDateTime.getHourOfDay() == 12) {
				postgreSQLPayTV.executeSQL(connection,
						PostgreSQLPayTV.generatedSQLDeleteOldRows(POSTGRESQL_PAYTV_TABLE_NOW, "date", 2));
			}
			if (hdfsIOSimple.isExist(getProcessFilePath(currentDateTime.plusHours(1)))) {
				updateUserUsage(getProcessFilePath(currentDateTime), currentDateTime);
			} else {
				listMissing.addAll(listDateString.subList(listDateString.indexOf(date), listDateString.size()));
				break;
			}
		}
		ParseLogService.printListParseMissing(listMissing);
		closePostgreSQLConnection();
	}
	
	public void processTableNowCreateTable(){
		openPostgreSQLConnection();
		postgreSQLPayTV.executeSQL(connection, PostgreSQLPayTV.SQL_CREATE_TABLE_NOW);
		closePostgreSQLConnection();
	}

	private String getProcessFilePath(DateTime dateTime) {
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = cf.get(CommonConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/" + String.format("%02d", month) + "/"
				+ String.format("%02d", day) + "/" + String.format("%02d", hour);
		path = path + "/" + year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "_"
				+ String.format("%02d", hour) + "_log_parsed.csv";
		return path;
	}

	public void updateUserUsage(String filePath, DateTime dateTime){
		Map<String, String> mapUserContract = userUsage.calculateUserUsageHourly(filePath, hdfsIOSimple);
		Map<String, Map<Integer, Integer>> mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		Map<String, Map<String, Integer>> mapUserVectorApp = userUsage.getMapUserVectorApp();
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();

		long start = System.currentTimeMillis();
		int countUpdate = 0;
		int countInsert = 0;

		for (String customerId : mapUserContract.keySet()) {
			Map<String, Integer> mapUsage = new HashMap<>();
			for (Integer hour : mapUserVectorHourly.get(customerId).keySet()) {
				mapUsage.put(Integer.toString(hour), mapUserVectorHourly.get(customerId).get(hour));
			}
			mapUsage.putAll(mapUserVectorApp.get(customerId));
			mapUserUsage.put(customerId, mapUsage);
		}

		long checkUpdate = 0;
		long checkInsert = 0;
		
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserContract.keySet(), 500);
		for (Set<String> subSetUser : listSetUser) {

			Map<String, Map<String, Integer>> mapUserUsageUpdate = postgreSQLPayTV.queryUserUsage(connection,
					POSTGRESQL_PAYTV_TABLE_NOW, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime), subSetUser);
			if (mapUserUsageUpdate.size() > 0) {
				for (String customerId : mapUserUsageUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringInteger(mapUserUsage.get(customerId),
							mapUserUsageUpdate.get(customerId));
					mapUserUsageUpdate.put(customerId, mapInfo);
				}
				long t = postgreSQLPayTV.updateUserUsageMultiple(connection, POSTGRESQL_PAYTV_TABLE_NOW, mapUserUsageUpdate,
						mapUserContract, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				checkUpdate += t;
				countUpdate += mapUserUsageUpdate.size();
			}

			Map<String, Map<String, Integer>> mapUserUsageInsert = new HashMap<>();
			for (String customerId : subSetUser) {
				if (!mapUserUsageUpdate.containsKey(customerId)) {
					mapUserUsageInsert.put(customerId, mapUserUsage.get(customerId));
				}
			}
			if (mapUserUsageInsert.size() > 0) {
				long t = postgreSQLPayTV.insertUserUsageMultiple(connection, POSTGRESQL_PAYTV_TABLE_NOW, mapUserUsageInsert,
						mapUserContract, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				checkInsert += t;
				countInsert += mapUserUsageInsert.size();
			}
		}
		PayTVUtils.LOG_INFO.info("============ Update Multiple | Number: " + countUpdate + " | Time: " + checkUpdate);
		PayTVUtils.LOG_INFO.info("Insert Multiple Time: " + checkInsert);
		
		PayTVUtils.LOG_INFO.info("Done update table now: Update: " + countUpdate + " | Insert: " + countInsert
				+ " | Time: " + (System.currentTimeMillis() - start));
		System.out.println("Done update table now: Update: " + countUpdate + " | Insert: " + countInsert + " | Time: "
				+ (System.currentTimeMillis() - start));
	}
}
