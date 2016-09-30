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
import org.joda.time.DateTimeComparator;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.db.TableAppDAO;
import com.fpt.ftel.paytv.statistic.UserUsageDetailApp;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceTableApp {
	private UserUsageDetailApp userUsageDetailApp;
	private HdfsIO hdfsIO;
	private TableAppDAO tableAppDAO;
	private static String status;

	public static void main(String[] args) throws IOException {
		System.out.println("- Fix command: java -jar .jar fix yyyy-mm-dd_HH yyyy-mm-dd_HH");
		System.out.println("note: fix [from day dd date1 to day dd date2] ");
		ServiceTableApp serviceTableApp = new ServiceTableApp();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table ..........");
			try {
				serviceTableApp.processCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start real job ..........");
			try {
				serviceTableApp.processTableReal(args[1]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("fix") && args.length == 3) {
			System.out.println("Start table now fix job ..........");
			try {
				serviceTableApp.processTableFix(args[1], args[2]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceTableApp() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableAppService.properties");
		try {
			hdfsIO = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE), CommonConfig.get(PayTVConfig.HDFS_SITE));
		} catch (IOException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
			e.printStackTrace();
		}

		userUsageDetailApp = UserUsageDetailApp.getInstance();
		tableAppDAO = new TableAppDAO();
	}

	public void processCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableAppDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

	public void processTableFix(String fromDate, String toDate) throws IOException, SQLException {
		DateTime beginDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(fromDate).plusDays(1);
		DateTime endDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(toDate).plusDays(1);
		while (new Duration(beginDate, endDate).getStandardSeconds() >= 0) {
			processTableReal(PayTVUtils.FORMAT_DATE_TIME.print(beginDate));
			beginDate = beginDate.plusDays(1);
		}
	}

	public void processTableReal(String dateString) throws IOException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.TABLE_APP_SERVICE_MISSING);
		List<String> listMissing = new ArrayList<>();
		DateTime currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(currentDateTime.minusDays(1)));

		for (String date : listDateString) {
			currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);
			boolean willProcess = false;
			int wait = 0;
			while (willProcess == false && wait < 4) {
				willProcess = true;
				List<DateTime> listDateTimePreServiceUnprocessed = ServiceUtils
						.getListDateProcessMissing(ServiceUtils.TABLE_NOW_SERVICE_MISSING);
				for (DateTime dateTimeUnprocessed : listDateTimePreServiceUnprocessed) {
					if (DateTimeComparator.getDateOnlyInstance().compare(dateTimeUnprocessed, currentDateTime) == 0) {
						willProcess = false;
						break;
					}
				}
				if (willProcess == false) {
					wait++;
					try {
						Thread.sleep(30 * 60 * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if (willProcess) {
				status = "Start process day: " + currentDateTime;
				PayTVUtils.LOG_INFO.info(status);
				System.out.println(status);
				updateUserUsage(currentDateTime, connection);
			} else {
				listMissing.add(date);
			}
		}
		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.TABLE_APP_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	private List<String> getListFilePathFromDateTime(DateTime dateTime) throws IOException {
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		String path = CommonConfig.get(PayTVConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/"
				+ String.format("%02d", month) + "/" + String.format("%02d", day) + "/";
		return hdfsIO.getListFileInDir(path);
	}

	private void updateUserUsage(DateTime dateTime, Connection connection)
			throws UnsupportedEncodingException, IOException, SQLException {
		List<String> listFilePath = getListFilePathFromDateTime(dateTime);
		userUsageDetailApp.initMap();
		for (String filePath : listFilePath) {
			userUsageDetailApp.statisticUserUsageDetail(filePath, hdfsIO);
		}
		Map<String, String> mapUserContract = userUsageDetailApp.getMapUserContract();
		Map<String, Map<String, Map<Integer, Integer>>> mapUserUsageDetailHourly = userUsageDetailApp
				.getMapUserAppDetailHourly();
		Map<String, Map<String, Map<String, Integer>>> mapUserUsageDetailDaily = userUsageDetailApp
				.getMapUserAppDetailDaily();
		updateDB(connection, mapUserContract, mapUserUsageDetailHourly, mapUserUsageDetailDaily, dateTime);
	}
	
	private void updateDB(Connection connection, Map<String, String> mapUserContract,
			Map<String, Map<String, Map<Integer, Integer>>> mapUserUsageDetailHourly,
			Map<String, Map<String, Map<String, Integer>>> mapUserUsageDetailDaily, DateTime dateTime) throws SQLException {
		String currentDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		String dropDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime.minusDays(
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_PROFILE_WEEK_TIMETOLIVE))));
		tableAppDAO.dropPartitionWeek(connection, dropDateSimple);
		tableAppDAO.createPartitionWeek(connection, currentDateSimple);
		dropDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime.minusDays(
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_PROFILE_MONTH_TIMETOLIVE))));
		tableAppDAO.dropPartitionMonth(connection, dropDateSimple);
		tableAppDAO.createPartitionMonth(connection, currentDateSimple);
		
		int countUpdate = 0;
		int countInsert = 0;
		int countUpdateWeek = 0;
		int countInsertWeek = 0;
		int countUpdateMonth = 0;
		int countInsertMonth = 0;
		long start = System.currentTimeMillis();
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserUsageDetailHourly.keySet(), 500);

		for (Set<String> subSetUser : listSetUser) {
			Map<String, Map<String, Map<String, Integer>>> mapApp = joinMap(subSetUser, mapUserUsageDetailHourly,
					mapUserUsageDetailDaily);

			for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
				Map<String, Map<String, Integer>> mapUserUsage = mapApp.get(app);
				Set<String> setUser = mapUserUsage.keySet();
				// PROCESS SUM
				Map<String, Map<String, Integer>> mapUserUsageUpdate = tableAppDAO.queryUserUsage(connection, app,
						setUser);
				if (mapUserUsageUpdate.size() > 0) {
					for (String customerId : mapUserUsageUpdate.keySet()) {
						Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerEasy(mapUserUsage.get(customerId),
								mapUserUsageUpdate.get(customerId));
						mapUserUsageUpdate.put(customerId, mapInfo);
					}
					tableAppDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract, app);
					countUpdate += mapUserUsageUpdate.size();
				}

				Map<String, Map<String, Integer>> mapUserUsageInsert = new HashMap<>();
				for (String customerId : setUser) {
					if (!mapUserUsageUpdate.containsKey(customerId)) {
						mapUserUsageInsert.put(customerId, mapUserUsage.get(customerId));
					}
				}
				if (mapUserUsageInsert.size() > 0) {
					tableAppDAO.insertUserUsageMultiple(connection, mapUserUsageInsert, mapUserContract, app);
					countInsert += mapUserUsageInsert.size();
				}
				
				//PROCESS WEEK
				mapUserUsageUpdate = tableAppDAO.queryUserUsage(connection, app, setUser, currentDateSimple, "week");
				if (mapUserUsageUpdate.size() > 0) {
					for (String customerId : mapUserUsageUpdate.keySet()) {
						Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerEasy(mapUserUsage.get(customerId),
								mapUserUsageUpdate.get(customerId));
						mapUserUsageUpdate.put(customerId, mapInfo);
					}
					tableAppDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract, app, currentDateSimple, "week");
					countUpdateWeek += mapUserUsageUpdate.size();
				}

				mapUserUsageInsert = new HashMap<>();
				for (String customerId : setUser) {
					if (!mapUserUsageUpdate.containsKey(customerId)) {
						mapUserUsageInsert.put(customerId, mapUserUsage.get(customerId));
					}
				}
				if (mapUserUsageInsert.size() > 0) {
					tableAppDAO.insertUserUsageMultiple(connection, mapUserUsageInsert, mapUserContract, app, currentDateSimple, "week");
					countInsertWeek += mapUserUsageInsert.size();
				}
				
				//PROCESS MONTH
				mapUserUsageUpdate = tableAppDAO.queryUserUsage(connection, app, setUser, currentDateSimple, "month");
				if (mapUserUsageUpdate.size() > 0) {
					for (String customerId : mapUserUsageUpdate.keySet()) {
						Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerEasy(mapUserUsage.get(customerId),
								mapUserUsageUpdate.get(customerId));
						mapUserUsageUpdate.put(customerId, mapInfo);
					}
					tableAppDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract, app, currentDateSimple, "month");
					countUpdateMonth += mapUserUsageUpdate.size();
				}

				mapUserUsageInsert = new HashMap<>();
				for (String customerId : setUser) {
					if (!mapUserUsageUpdate.containsKey(customerId)) {
						mapUserUsageInsert.put(customerId, mapUserUsage.get(customerId));
					}
				}
				if (mapUserUsageInsert.size() > 0) {
					tableAppDAO.insertUserUsageMultiple(connection, mapUserUsageInsert, mapUserContract, app, currentDateSimple, "month");
					countInsertMonth += mapUserUsageInsert.size();
				}
			}
		}
		status = "Done update table app SUM: Update: " + countUpdate + " | Insert: " + countInsert;
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
		status = "Done update table app WEEK: Update: " + countUpdateWeek + " | Insert: " + countInsertWeek;
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
		status = "Done update table app MONTH: Update: " + countUpdateMonth + " | Insert: " + countInsertMonth;
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
		status = "====== Done update profile_app witn Time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis();
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}
	

	private void updateDB(Connection connection, Map<String, String> mapUserContract,
			Map<String, Map<String, Map<Integer, Integer>>> mapUserUsageDetailHourly,
			Map<String, Map<String, Map<String, Integer>>> mapUserUsageDetailDaily) throws SQLException {
		int countUpdate = 0;
		int countInsert = 0;
		long start = System.currentTimeMillis();
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserUsageDetailHourly.keySet(), 500);

		for (Set<String> subSetUser : listSetUser) {
			Map<String, Map<String, Map<String, Integer>>> mapApp = joinMap(subSetUser, mapUserUsageDetailHourly,
					mapUserUsageDetailDaily);

			for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
				Map<String, Map<String, Integer>> mapUserUsage = mapApp.get(app);
				Set<String> setUser = mapUserUsage.keySet();
				Map<String, Map<String, Integer>> mapUserUsageUpdate = tableAppDAO.queryUserUsage(connection, app,
						setUser);
				if (mapUserUsageUpdate.size() > 0) {
					for (String customerId : mapUserUsageUpdate.keySet()) {
						Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerEasy(mapUserUsage.get(customerId),
								mapUserUsageUpdate.get(customerId));
						mapUserUsageUpdate.put(customerId, mapInfo);
					}
					tableAppDAO.updateUserUsageMultiple(connection, mapUserUsageUpdate, mapUserContract, app);
					countUpdate += mapUserUsageUpdate.size();
				}

				Map<String, Map<String, Integer>> mapUserUsageInsert = new HashMap<>();
				for (String customerId : setUser) {
					if (!mapUserUsageUpdate.containsKey(customerId)) {
						mapUserUsageInsert.put(customerId, mapUserUsage.get(customerId));
					}
				}
				if (mapUserUsageInsert.size() > 0) {
					tableAppDAO.insertUserUsageMultiple(connection, mapUserUsageInsert, mapUserContract, app);
					countInsert += mapUserUsageInsert.size();
				}
			}
		}
		status = "Done update table app: Update: " + countUpdate + " | Insert: " + countInsert + " | Time: "
				+ (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis();
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

	private Map<String, Map<String, Map<String, Integer>>> joinMap(Set<String> setCustomerId,
			Map<String, Map<String, Map<Integer, Integer>>> mapUserUsageDetailHourly,
			Map<String, Map<String, Map<String, Integer>>> mapUserUsageDetailDaily) {
		Map<String, Map<String, Map<String, Integer>>> result = new HashMap<>();
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			Map<String, Map<String, Integer>> temp = new HashMap<>();
			for (String customerId : setCustomerId) {
				Map<String, Integer> info = new HashMap<>();
				if (mapUserUsageDetailHourly.get(customerId).containsKey(app)) {
					Map<Integer, Integer> mapH = mapUserUsageDetailHourly.get(customerId).get(app);
					Map<String, Integer> mapD = mapUserUsageDetailDaily.get(customerId).get(app);
					info.putAll(PayTVDBUtils.formatDBVectorHourly(mapH));
					info.putAll(PayTVDBUtils.formatDBVectorDaily(mapD));
					temp.put(customerId, info);
				}
			}
			result.put(app, temp);
		}
		return result;
	}

}
