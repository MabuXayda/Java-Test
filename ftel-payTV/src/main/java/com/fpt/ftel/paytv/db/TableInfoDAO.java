package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableInfoDAO {

	private static final String SQL_CREATE_TABLE_INFO = "CREATE TABLE IF NOT EXISTS info "
			+ "(contract VARCHAR(22), customer_id VARCHAR(22), " + "service_id INT, service_name TEXT, "
			+ "location TEXT, " + "status_id INT, start_date DATE, stop_date DATE, " + "last_active DATE, "
			+ "PRIMARY KEY(customer_id, contract));";

	public void createTable(Connection connection) throws SQLException {
		System.out.println(SQL_CREATE_TABLE_INFO);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_INFO);
	}

	public void updateLastActive(Connection connection, Set<String> setUser, DateTime dateTime) throws SQLException {
		String dateString = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		String sql = "UPDATE info AS t SET last_active = c.last_active FROM (VALUES ";
		for (String user : setUser) {
			sql = sql + "('" + user + "','" + dateString + "'),";
		}
		sql = sql.substring(0, sql.length() - 1)
				+ ") AS c (customer_id, lastinfo_active) WHERE c.customer_id = t.customer_id;";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateChurnStatus(Connection connection, Map<String, Map<String, String>> mapUserChurnInfo)
			throws SQLException {
		String sql = "UPDATE info AS t SET status_id = c.status_id, stop_date = c.stop_date FROM (VALUES ";
		for (String customer_id : mapUserChurnInfo.keySet()) {
			Map<String, String> info = mapUserChurnInfo.get(customer_id);
			sql = sql + "('" + customer_id + "'," + info.get(UserStatus.STATUS_ID) + ",'"
					+ info.get(UserStatus.STOP_DATE + "'),");
		}
		sql = sql.substring(0, sql.length() - 1)
				+ ") AS c(customer_id, status_id, stop_date) WHERE c.customer_id = t.customer_id;";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertNewUser(Connection connection, Map<String, Map<String, String>> mapUserRegisterInfo)
			throws SQLException {
		String sql = "INSERT INTO info (contract, customer_id, location, status_id, start_date) VALUES ";
		for (String customer_id : mapUserRegisterInfo.keySet()) {
			Map<String, String> info = mapUserRegisterInfo.get(customer_id);
			sql = sql + "('" + info.get(UserStatus.CONTRACT) + "','" + customer_id + "','"
					+ info.get(UserStatus.LOCATION) + "'," + info.get(UserStatus.STATUS_ID) + ",'"
					+ info.get(UserStatus.START_DATE) + "'),";
		}
		sql = sql.substring(0, sql.length() - 1) + ";";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

}
