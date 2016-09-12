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
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.db.TableNowDAO;
import com.fpt.ftel.paytv.statistic.UserUsage;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceTableNow {
	private UserUsage userUsage;
	private HdfsIO hdfsIO;
	private TableNowDAO tableNowDAO;

	public static void main(String[] args) {
		ServiceTableNow tableNowService = new ServiceTableNow();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table now ..........");
			try {
				tableNowService.processTableNowCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start table now real job ..........");
			try {
				tableNowService.processTableNowReal(args[1]);
			} catch (IOException | NumberFormatException | SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceTableNow() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableNowService.properties");
		try {
			hdfsIO = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE), CommonConfig.get(PayTVConfig.HDFS_SITE));
		} catch (IOException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
			e.printStackTrace();
		}
		userUsage = new UserUsage();
		tableNowDAO = new TableNowDAO();
	}

	public void processTableNowReal(String dateString) throws IOException, NumberFormatException, SQLException {
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
				tableNowDAO.deleteOldRecords(connection,
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

	public void processTableNowCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableNowDAO.createTable(connection);
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

	private void updateUserUsage(DateTime dateTime, Connection connection) throws SQLException {
		String filePath = getFilePathFromDateTime(dateTime);
		Map<String, String> mapUserContract = userUsage.calculateUserUsageService(filePath, hdfsIO);
		Map<String, Map<Integer, Integer>> mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		Map<String, Map<String, Integer>> mapUserVectorApp = userUsage.getMapUserVectorApp();
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();

		int countUpdate = 0;
		int countInsert = 0;

		for (String customerId : mapUserContract.keySet()) {
			// System.out.print(customerId);
			// for (Integer key : mapUserVectorHourly.get(customerId).keySet())
			// {
			// System.out.print("| " + key + ":" +
			// mapUserVectorHourly.get(customerId).get(key));
			// }
			// System.out.println();

			Map<String, Integer> mapUsage = new HashMap<>();
			mapUsage.putAll(PayTVDBUtils.formatDBVectorHourly(mapUserVectorHourly.get(customerId)));
			mapUsage.putAll(PayTVDBUtils.formatDBVectorApp(mapUserVectorApp.get(customerId)));
			mapUserUsage.put(customerId, mapUsage);
			// System.out.print(customerId);
			// for(String key : mapUsage.keySet()){
			// System.out.print("|" + key + ":" + mapUsage.get(key));
			// }
			// System.out.println();
		}

		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserContract.keySet(), 500);

		long start = System.currentTimeMillis();
		for (Set<String> subSetUser : listSetUser) {

			Map<String, Map<String, Integer>> mapUserUsageUpdate = tableNowDAO.queryUserUsage(connection,
					PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime), subSetUser);
			if (mapUserUsageUpdate.size() > 0) {
				for (String customerId : mapUserUsageUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerHard(mapUserUsage.get(customerId),
							mapUserUsageUpdate.get(customerId));
					mapUserUsageUpdate.put(customerId, mapInfo);
				}
				tableNowDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract,
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
				tableNowDAO.insertUserUsageMultiple(connection, mapUserUsageInsert, mapUserContract,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				countInsert += mapUserUsageInsert.size();
			}
		}

		updateUserUsageDump(dateTime.minusDays(1), connection);

		PayTVUtils.LOG_INFO.info("Done update table now: Update: " + countUpdate + " | Insert: " + countInsert
				+ " | Time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
		System.out.println("Done update table now: Update: " + countUpdate + " | Insert: " + countInsert + " | Time: "
				+ (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
	}

	private void updateUserUsageDump(DateTime dateTime, Connection connection) throws SQLException {
		Map<String, String> mapUserContract = userUsage.processServiceDump();
		Map<String, Map<Integer, Integer>> mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		Map<String, Map<String, Integer>> mapUserVectorApp = userUsage.getMapUserVectorApp();
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		int countUpdate = 0;
		int countInsert = 0;

		for (String customerId : mapUserContract.keySet()) {
			Map<String, Integer> mapUsage = new HashMap<>();
			mapUsage.putAll(PayTVDBUtils.formatDBVectorHourly(mapUserVectorHourly.get(customerId)));
			mapUsage.putAll(PayTVDBUtils.formatDBVectorApp(mapUserVectorApp.get(customerId)));
			mapUserUsage.put(customerId, mapUsage);
		}

		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserContract.keySet(), 500);

		long start = System.currentTimeMillis();
		for (Set<String> subSetUser : listSetUser) {

			Map<String, Map<String, Integer>> mapUserUsageUpdate = tableNowDAO.queryUserUsage(connection,
					PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime), subSetUser);
			if (mapUserUsageUpdate.size() > 0) {
				for (String customerId : mapUserUsageUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerHard(mapUserUsage.get(customerId),
							mapUserUsageUpdate.get(customerId));
					mapUserUsageUpdate.put(customerId, mapInfo);
				}
				tableNowDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract,
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
				tableNowDAO.insertUserUsageMultiple(connection, mapUserUsageInsert, mapUserContract,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				countInsert += mapUserUsageInsert.size();
			}
		}

		PayTVUtils.LOG_INFO.info("Done update table now dump: Update: " + countUpdate + " | Insert: " + countInsert
				+ " | Time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
		System.out.println("Done update table now dump: Update: " + countUpdate + " | Insert: " + countInsert
				+ " | Time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
	}

}
