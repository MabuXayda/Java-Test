package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.Duration;

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
	private static String status;

	public static void main(String[] args) {
		System.out.println("- Fix command: java -jar .jar fix yyyy-mm-dd_HH yyyy-mm-dd_HH");
		System.out.println("note: fix [from hour HH date1 to hour HH date2] ");
		ServiceTableNow tableNowService = new ServiceTableNow();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table now ..........");
			try {
				tableNowService.processCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start table now real job ..........");
			try {
				tableNowService.processTableReal(args[1]);
			} catch (IOException | NumberFormatException | SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("fix") && args.length == 3) {
			System.out.println("Start table now fix job ..........");
			try {
				tableNowService.processTableFix(args[1], args[2]);
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
		userUsage = UserUsage.getInstance();
		tableNowDAO = new TableNowDAO();
	}

	public void processTableReal(String dateString) throws IOException, NumberFormatException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));

		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.TABLE_NOW_SERVICE_MISSING);
		List<String> listMissing = new ArrayList<>();
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString).minusHours(1)));

		for (String date : listDateString) {
			DateTime processDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);

			if (processDateTime.getHourOfDay() == 12) {
				tableNowDAO.deleteOldRecords(connection,
						Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_NOW_TIMETOLIVE)));
			}

			if (hdfsIO.isExist(getFilePathFromDateTime(processDateTime.plusHours(1)))) {
				boolean willProcess = true;
				List<DateTime> listDateTimePreServiceUnprocessed = ServiceUtils
						.getListDateProcessMissing(ServiceUtils.PARSE_LOG_SERVICE_MISSING);
				for (DateTime dateTimeUnprocessed : listDateTimePreServiceUnprocessed) {
					if (DateTimeUtils.compareToHour(dateTimeUnprocessed, processDateTime) == 0) {
						willProcess = false;
						break;
					}
				}
				if (willProcess) {
					status = "Start process day: " + processDateTime;
					PayTVUtils.LOG_INFO.info(status);
					System.out.println(status);
					updateUserUsage(processDateTime, connection);
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

	public void processTableFix(String fromDate, String toDate)
			throws NumberFormatException, IOException, SQLException {
		DateTime beginDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(fromDate).plusHours(1);
		DateTime endDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(toDate).plusHours(1);
		while (new Duration(beginDate, endDate).getStandardSeconds() >= 0) {
			processTableReal(PayTVUtils.FORMAT_DATE_TIME.print(beginDate));
			beginDate = beginDate.plusHours(1);
		}
	}

	public void processCreateTable() throws SQLException {
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

	private void updateUserUsage(DateTime dateTime, Connection connection)
			throws UnsupportedEncodingException, IOException, SQLException {
		String filePath = getFilePathFromDateTime(dateTime);
		// ================== FIRST UPDATE
		userUsage.initExtra();
		userUsage.initMap();
		userUsage.statisticUserUsage(filePath, hdfsIO);
		Map<String, String> mapUserContract = userUsage.getMapUserContract();
		Map<String, Map<Integer, Integer>> mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		Map<String, Map<String, Integer>> mapUserVectorApp = userUsage.getMapUserVectorApp();
		updateDB(dateTime, connection, mapUserContract, mapUserVectorHourly, mapUserVectorApp);
		// ================== EXTRA UPDATE
		userUsage.initMap();
		userUsage.statisticUserUsageExtra();
		mapUserContract = userUsage.getMapUserContract();
		mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		mapUserVectorApp = userUsage.getMapUserVectorApp();
		updateDB(dateTime.minusDays(1), connection, mapUserContract, mapUserVectorHourly, mapUserVectorApp);
	}

	private void updateDB(DateTime dateTime, Connection connection, Map<String, String> mapUserContract,
			Map<String, Map<Integer, Integer>> mapUserVectorHourly, Map<String, Map<String, Integer>> mapUserVectorApp)
			throws SQLException {
		int countUpdate = 0;
		int countInsert = 0;
		long start = System.currentTimeMillis();
		// ================== SPLIT
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserVectorHourly.keySet(), 500);
		for (Set<String> subSetUser : listSetUser) {
			Map<String, Map<String, Integer>> mapUserUsage = joinMap(subSetUser, mapUserVectorHourly, mapUserVectorApp);
			Map<String, Map<String, Integer>> mapUserUsageUpdate = tableNowDAO.queryUserUsage(connection,
					PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime), subSetUser);
			// ================== UPDATE
			if (mapUserUsageUpdate.size() > 0) {
				for (String customerId : mapUserUsageUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerEasy(mapUserUsage.get(customerId),
							mapUserUsageUpdate.get(customerId));
					mapUserUsageUpdate.put(customerId, mapInfo);
				}
				tableNowDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
				countUpdate += mapUserUsageUpdate.size();
			}
			// ================== INSERT
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
		status = "Done update table now: Update: " + countUpdate + " | Insert: " + countInsert + " | Time: "
				+ (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis();
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

	private Map<String, Map<String, Integer>> joinMap(Set<String> setCustomerId,
			Map<String, Map<Integer, Integer>> mapUserVectorHourly,
			Map<String, Map<String, Integer>> mapUserVectorApp) {
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		for (String customerId : setCustomerId) {
			Map<String, Integer> mapUsage = new HashMap<>();
			mapUsage.putAll(PayTVDBUtils.formatDBVectorHourly(mapUserVectorHourly.get(customerId)));
			mapUsage.putAll(PayTVDBUtils.formatDBVectorApp(mapUserVectorApp.get(customerId)));
			mapUserUsage.put(customerId, mapUsage);
		}
		return mapUserUsage;
	}
}
