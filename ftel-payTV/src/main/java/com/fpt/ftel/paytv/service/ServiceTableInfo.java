package com.fpt.ftel.paytv.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.paytv.db.TableInfoDAO;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceTableInfo {
	private TableInfoDAO tableInfoDAO;
	private static String status;

	public static void main(String[] args) throws IOException, SQLException {
		System.out.println("- Fix command: java -jar .jar fix yyyy-mm-dd_HH yyyy-mm-dd_HH");
		System.out.println("note: fix [from day dd date1 to day dd date2] ");
		ServiceTableInfo serviceTableInfo = new ServiceTableInfo();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table ..........");
			try {
				serviceTableInfo.processCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("first load") && args.length == 1) {
			System.out.println("Start first load job ..........");
			try {
				serviceTableInfo.processFirstLoad();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start real job ..........");
			try {
				serviceTableInfo.processTableReal(args[1]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("fix") && args.length == 3) {
			System.out.println("Start table now fix job ..........");
			try {
				serviceTableInfo.processTableFix(args[1], args[2]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceTableInfo() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableInfoService.properties");
		tableInfoDAO = new TableInfoDAO();
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

		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.TABLE_INFO_SERVICE_MISSING);
		List<String> listMissing = new ArrayList<>();
		listDateString.add(
				PayTVUtils.FORMAT_DATE_TIME.print(PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString).minusDays(1)));
		for (String date : listDateString) {
			DateTime processDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);
			if (ServiceUtils.willProcessCompareToDay(ServiceUtils.TABLE_NOW_SERVICE_MISSING, processDateTime)) {
				status = "Start process day: " + processDateTime;
				PayTVUtils.LOG_INFO.info(status);
				System.out.println(status);
				insertRegister(connection, processDateTime);
				updateChurn(connection, processDateTime);
				updateLastActive(connection, processDateTime);
			} else {
				listMissing.add(date);
			}
		}
		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.TABLE_INFO_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	private void insertRegister(Connection connection, DateTime dateTime) throws IOException, SQLException {
		URL url = new URL(CommonConfig.get(PayTVConfig.GET_USER_REGISTER_API)
				+ PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
		String content = IOUtils.toString(url, "UTF-8");
		Map<String, Map<String, String>> mapRegister = UserStatus.getMapUserRegisterApi(content, dateTime);
		if (mapRegister != null && mapRegister.size() > 0) {
			tableInfoDAO.insertNewUser(connection, mapRegister);
		}
		status = "Done insert user REGISTER at day: " + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime)
				+ " | Count: " + mapRegister.size();
		System.out.println(status);
		PayTVUtils.LOG_INFO.info(status);
	}

	private void updateChurn(Connection connection, DateTime dateTime) throws IOException, SQLException {
		URL url = new URL(
				CommonConfig.get(PayTVConfig.GET_USER_CHURN_API) + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
		String content = IOUtils.toString(url, "UTF-8");
		Map<String, Map<String, String>> mapChurn = UserStatus.getMapUserChurnApi(content, dateTime);
		if (mapChurn != null && mapChurn.size() > 0) {
			tableInfoDAO.updateChurnStatus(connection, mapChurn);
		}
		status = "Done update user CHURN at day: " + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime) + " | Count: "
				+ mapChurn.size();
		System.out.println(status);
		PayTVUtils.LOG_INFO.info(status);
	}

	private void updateLastActive(Connection connection, DateTime dateTime) throws SQLException {
		String dateString = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		Map<String, String> mapLastActive = tableInfoDAO.getLastActive(connection, dateString);
		if (mapLastActive != null && mapLastActive.size() > 0) {
			List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapLastActive.keySet(), 500);
			for (Set<String> setUser : listSetUser) {
				Map<String, String> smallerMap = new HashMap<>();
				for (String id : setUser) {
					smallerMap.put(id, mapLastActive.get(id));
				}
				tableInfoDAO.updateLastActive(connection, smallerMap);
			}
		}
		status = "Done update user LAST ACTIVE at day: " + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime)
				+ " | Count: " + mapLastActive.size();
		System.out.println(status);
		PayTVUtils.LOG_INFO.info(status);
	}

	public void processFirstLoad() throws IOException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));

		Map<String, Map<String, String>> mapInsert = new HashMap<>();
		Map<String, Map<String, String>> mapUpdate = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(CommonConfig.get(PayTVConfig.USER_INFO_FIRST_LOAD_FILE)));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			String customerId = arr[1];
			Map<String, String> insert = new HashMap<>();
			insert.put(UserStatus.CONTRACT, arr[0]);
			insert.put(UserStatus.LOCATION_ID, arr[2]);
			insert.put(UserStatus.LOCATION, arr[3]);
			insert.put(UserStatus.REGION, arr[4]);
			insert.put(UserStatus.STATUS_ID, Integer.toString(1));
			insert.put(UserStatus.START_DATE, arr[6]);
			mapInsert.put(customerId, insert);
			if (arr.length == 8) {
				Map<String, String> update = new HashMap<>();
				update.put(UserStatus.STATUS_ID, arr[5]);
				update.put(UserStatus.STOP_DATE, arr[7]);
				mapUpdate.put(customerId, update);
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Done load");
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapInsert.keySet(), 500);
		for (Set<String> setuser : listSetUser) {
			Map<String, Map<String, String>> subMap = new HashMap<>();
			for (String id : setuser) {
				subMap.put(id, mapInsert.get(id));
			}
			tableInfoDAO.insertNewUser(connection, subMap);
		}
		System.out.println("Done first insert: " + mapInsert.size());

		listSetUser = ListUtils.splitSetToSmallerSet(mapUpdate.keySet(), 500);
		for (Set<String> setuser : listSetUser) {
			Map<String, Map<String, String>> subMap = new HashMap<>();
			for (String id : setuser) {
				subMap.put(id, mapUpdate.get(id));
			}
			tableInfoDAO.updateChurnStatus(connection, subMap);
		}
		System.out.println("Done first update: " + mapUpdate.size());

		ConnectionFactory.closeConnection(connection);
	}

	public void processCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableInfoDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

}
