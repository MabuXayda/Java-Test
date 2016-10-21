package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.db.TableCallLogDAO;
import com.fpt.ftel.paytv.statistic.UserCall;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceTableCallLog {
	private UserCall userCall;
	private TableCallLogDAO tableCallLogDAO;
	private String status;

	public static void main(String[] args) {
		ServiceTableCallLog serviceTableCallLog = new ServiceTableCallLog();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table ..........");
			try {
				serviceTableCallLog.processCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("insert") && args.length == 1) {
			System.out.println("Start real job ..........");
			try {
				for (int i = 2; i < 9; i++) {
					String filePath = "/build/payTV/CALL_LOG/call_log_2016-" + NumberUtils.get2CharNumber(i) + ".txt";
					String date = "2016-" + NumberUtils.get2CharNumber(i) + "-01";
					serviceTableCallLog.processInsert(date, filePath);
				}
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceTableCallLog() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableCallLogService.properties");
		userCall = new UserCall();
		tableCallLogDAO = new TableCallLogDAO();
	}

	public void processCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableCallLogDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

	public void processInsert(String dateString, String path) throws IOException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		userCall.processRawLog(path);
		Map<String, Map<String, Integer>> mapContractCallCount = userCall.getMapContractCallCount();
		Map<String, Map<String, Integer>> mapContractCallDuration = userCall.getMapContractCallDuration();
		Map<String, Double> mapContractRecallAvg = userCall.getMapContractRecallAvg();
		DateTime dateTime = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		insertDB(connection, mapContractCallCount, mapContractCallDuration, mapContractRecallAvg, dateTime);
		ConnectionFactory.closeConnection(connection);
	}

	private void insertDB(Connection connection, Map<String, Map<String, Integer>> mapContractCallCount,
			Map<String, Map<String, Integer>> mapContractCallDuration, Map<String, Double> mapContractRecallAvg,
			DateTime dateTime) throws SQLException {
		String currentDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		tableCallLogDAO.createPartitionMonth(connection, currentDateSimple);
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapContractCallCount.keySet(), 300);

		int countInsert = 0;

		for (Set<String> setUser : listSetUser) {
			Map<String, Map<String, Integer>> subMapContractCallCount = new HashMap<>();
			Map<String, Map<String, Integer>> subMapContractCallDuration = new HashMap<>();
			Map<String, Double> subMapContractRecallAvg = new HashMap<>();
			for (String user : setUser) {
				subMapContractCallCount.put(user, mapContractCallCount.get(user));
				subMapContractCallDuration.put(user, mapContractCallDuration.get(user));
				subMapContractRecallAvg.put(user,
						mapContractRecallAvg.get(user) == null ? 0 : mapContractRecallAvg.get(user));
			}
			tableCallLogDAO.insertUserUsageMultiple(connection, subMapContractCallCount, subMapContractCallDuration,
					subMapContractRecallAvg, currentDateSimple);
			countInsert += setUser.size();
		}
		status = "------> Done insertDB CALL_LOG | Insert: " + countInsert;
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

}
