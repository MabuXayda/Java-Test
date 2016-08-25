package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.config.PayTVConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.db.SQLTableNow;
import com.fpt.ftel.paytv.statistic.UserUsage;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class TableNowService {
	private UserUsage userUsage;
	private HdfsIO hdfsIO;
	private static final String TABLE = "now";

	public static void main(String[] args) throws IOException{
		TableNowService tableNowService = new TableNowService();
		// tableNowService.test("2016-08-21");
		if (args[0].equals("test") && args.length == 2) {
			System.out.println("Start table now test job ..........");
			tableNowService.test(args[1]);
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start table now real job ..........");
			tableNowService.processTableNowReal(args[1]);
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public void test(String dateString) {
		for (int i = 0; i < 24; i++) {
			try {
				processTableNowReal(dateString + " " + NumberUtils.get2CharNumber(i) + ":10:00");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public TableNowService() throws IOException {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableNowService.properties");
		hdfsIO = new HdfsIO();
		userUsage = new UserUsage();
	}

	public void processTableNowReal(String dateString) throws IOException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));

		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.TABLE_NOW_SERVICE_MISSING);
		List<String> listMissing = new ArrayList<>();
		DateTime currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(currentDateTime.minusHours(1)));

		for (String date : listDateString) {
			currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);

			if (currentDateTime.getHourOfDay() == 12) {
				SQLTableNow.deleteOldRows(connection, TABLE,
						Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_NOW_TIMETOLIVE)));
			}

			if (hdfsIO.isExist(getFilePathFromDateTime(currentDateTime.plusHours(1)))) {
				boolean willProcess = true;
				List<DateTime> listDateTimePreServiceUnprocessed = ServiceUtils
						.getListDateProcessMissing(ServiceUtils.PARSE_LOG_SERVICE_MISSING);
				for (DateTime dateTimeUnprocessed : listDateTimePreServiceUnprocessed) {
					if (DateTimeUtils.compareToHour(dateTimeUnprocessed, currentDateTime) == 0) {
						willProcess = false;
						break;
					}
				}
				if (willProcess) {
					updateUserUsage(currentDateTime, connection);
				} else {
					listMissing.add(date);
				}
			} else {
				listMissing.add(date);
			}
		}

		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.TABLE_NOW_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	public void processTableNowCreateTable() {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		SQLTableNow.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

	private String getFilePathFromDateTime(DateTime dateTime) {
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = CommonConfig.get(PayTVConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/"
				+ String.format("%02d", month) + "/" + String.format("%02d", day) + "/" + String.format("%02d", hour);
		path = path + "/" + year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "_"
				+ String.format("%02d", hour) + "_log_parsed.csv";
		return path;
	}

	public void updateUserUsage(DateTime dateTime, Connection connection) {
		String filePath = getFilePathFromDateTime(dateTime);
		Map<String, String> mapUserContract = userUsage.calculateUserUsageHourly(filePath, hdfsIO);
		Map<String, Map<Integer, Integer>> mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		Map<String, Map<String, Integer>> mapUserVectorApp = userUsage.getMapUserVectorApp();
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();

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

		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserContract.keySet(), 500);

		long start = System.currentTimeMillis();
		for (Set<String> subSetUser : listSetUser) {

			Map<String, Map<String, Integer>> mapUserUsageUpdate = SQLTableNow.queryUserUsage(connection, TABLE,
					PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime), subSetUser);
			if (mapUserUsageUpdate.size() > 0) {
				for (String customerId : mapUserUsageUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringInteger(mapUserUsage.get(customerId),
							mapUserUsageUpdate.get(customerId));
					mapUserUsageUpdate.put(customerId, mapInfo);
				}
				long t = SQLTableNow.updateUserUsageMultiple(connection, TABLE, mapUserUsageUpdate, mapUserContract,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				countUpdate += mapUserUsageUpdate.size();
			}

			Map<String, Map<String, Integer>> mapUserUsageInsert = new HashMap<>();
			for (String customerId : subSetUser) {
				if (!mapUserUsageUpdate.containsKey(customerId)) {
					mapUserUsageInsert.put(customerId, mapUserUsage.get(customerId));
				}
			}
			if (mapUserUsageInsert.size() > 0) {
				long t = SQLTableNow.insertUserUsageMultiple(connection, TABLE, mapUserUsageInsert, mapUserContract,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				countInsert += mapUserUsageInsert.size();
			}
		}

		PayTVUtils.LOG_INFO.info("Done update table now: Update: " + countUpdate + " | Insert: " + countInsert
				+ " | Time: " + (System.currentTimeMillis() - start));
		System.out.println("Done update table now: Update: " + countUpdate + " | Insert: " + countInsert + " | Time: "
				+ (System.currentTimeMillis() - start));
	}

}
