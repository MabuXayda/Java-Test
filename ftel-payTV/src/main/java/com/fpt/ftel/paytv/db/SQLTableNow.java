package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class SQLTableNow {
	
	private static final String SQL_CREATE_TABLE_NOW = "CREATE TABLE now (contract VARCHAR(22), customer_id VARCHAR(22), date DATE, "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "PRIMARY KEY(customer_id, date));";
	
	public static void createTable(Connection connection){
		try {
			PostgreSQL.executeSQL(connection, SQL_CREATE_TABLE_NOW);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void deleteOldRows(Connection connection, String table, int days){
		try {
			PostgreSQL.executeSQL(connection, PostgreSQL.generatedSQLDeleteOldRows(table, "date", days));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static long insertUserUsageMultiple(Connection connection, String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		long start = System.currentTimeMillis();
		String sql = generatedSQLInsertUserUsageMultiple(table, mapUserUsage, mapUserContract, date);
		try {
			PostgreSQL.executeSQL(connection, sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis() - start;
	}

	public static long updateUserUsageMultiple(Connection connection, String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		long start = System.currentTimeMillis();
		String sql = generatedSQLUpdateUserUsageMultiple(table, mapUserUsage, mapUserContract, date);
		try {
			PostgreSQL.executeSQL(connection, sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return System.currentTimeMillis() - start;
	}
	
	public static Map<String, Map<String, Integer>> queryUserUsage(Connection connection, String table, String date,
			Set<String> setUser) {

		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		String sql = "SELECT * FROM " + table + " WHERE date = '" + date + "' AND customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				String customer_id = resultSet.getString("customer_id");
				Map<String, Integer> mapInfo = new HashMap<>();
				for (int i = 0; i < 24; i++) {
					mapInfo.put(Integer.toString(i),
							resultSet.getInt(PayTVDBParam.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i)));
				}
				for (String app : PayTVUtils.SET_APP_NAME_RTP) {
					mapInfo.put(app, resultSet.getInt(PayTVDBParam.VECTOR_APP_PREFIX + app.toLowerCase()));
				}
				mapUserUsage.put(customer_id, mapInfo);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapUserUsage;
	}

	private static String generatedSQLUpdateUserUsageSingle(String table, String customer_id,
			Map<String, Integer> mapUsage, String date) {
		String sql = "UPDATE " + table + " SET date = '" + date + "'";
		for (int i = 0; i < 24; i++) {
			sql = sql + "," + PayTVDBParam.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + " = "
					+ mapUsage.get(Integer.toString(i));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			sql = sql + "," + PayTVDBParam.VECTOR_APP_PREFIX + app.toLowerCase() + " = " + mapUsage.get(app);
		}
		sql = sql + " WHERE date = '" + date + "' AND customer_id = '" + customer_id + "';";
		return sql;
	}

	private static String generatedSQLUpdateUserUsageMultiple(String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		String sql = "UPDATE " + table + " AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId),
					date);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn()
				+ "WHERE c.customer_id = t.customer_id AND c.date::date = t.date;";
	}

	private static String generatedSQLInsertUserUsageSingle(String table, String customer_id, String contract,
			Map<String, Integer> mapUsage, String date) {
		String sql = "INSERT INTO " + table + " " + generatedStringColumn() + " VALUES "
				+ generatedStringValue(customer_id, contract, mapUsage, date) + ";";
		return sql;
	}

	private static String generatedSQLInsertUserUsageMultiple(String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		String sql = "INSERT INTO " + table + " " + generatedStringColumn() + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			sql = sql + generatedStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId),
					date) + ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private static String generatedStringSetCommand() {
		String setCommand = "SET ";
		for (int i = 0; i < 24; i++) {
			setCommand = setCommand + PayTVDBParam.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + " = c."
					+ PayTVDBParam.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + ",";
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			setCommand = setCommand + PayTVDBParam.VECTOR_APP_PREFIX + app.toLowerCase() + " = c."
					+ PayTVDBParam.VECTOR_APP_PREFIX + app.toLowerCase() + ",";
		}
		return setCommand.substring(0, setCommand.length() - 1);
	}

	private static String generatedStringValue(String customer_id, String contract, Map<String, Integer> mapUsage,
			String date) {
		String value = "('" + contract + "','" + customer_id + "','" + date + "'";
		for (int i = 0; i < 24; i++) {
			value = value + "," + (mapUsage.get(Integer.toString(i)) == null ? 0 : mapUsage.get(Integer.toString(i)));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			value = value + "," + (mapUsage.get(app) == null ? 0 : mapUsage.get(app));
		}
		value = value + ")";
		return value;
	}

	private static String generatedStringColumn() {
		String col = "(contract,customer_id,date";
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBParam.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			col = col + "," + PayTVDBParam.VECTOR_APP_PREFIX + app;
		}
		col = col + ")";
		return col;
	}

}
