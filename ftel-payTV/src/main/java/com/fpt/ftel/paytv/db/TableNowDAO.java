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
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableNowDAO {
	private static final String SQL_CREATE_TABLE_NOW = "CREATE TABLE IF NOT EXISTS now "
			+ "(customer_id VARCHAR(22), date DATE, "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "PRIMARY KEY(customer_id, date));";

	public void createTable(Connection connection) throws SQLException {
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_NOW);
	}

	public void deleteOldRecords(Connection connection, int days) throws SQLException {
		String sql = "DELETE FROM now WHERE date < NOW() - INTERVAL '" + days + " days';";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			String date) throws SQLException {
		String sql = generatedSQLInsertUserUsageMultiple(mapUserUsage, date);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			String date) throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultiple(mapUserUsage, date);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public Map<String, Map<String, Integer>> queryUserUsage(Connection connection, String date, Set<String> setUser)
			throws SQLException {
		PostgreSQL.setConstraintExclusion(connection, true);
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		String sql = "SELECT * FROM now WHERE date = '" + date + "' AND customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			Map<String, Integer> mapInfo = new HashMap<>();
			for (int i = 0; i < 24; i++) {
				String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
				mapInfo.put(key, resultSet.getInt(key));
			}
			for (String app : PayTVUtils.SET_APP_NAME_RTP) {
				String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
				mapInfo.put(key, resultSet.getInt(key));
			}
			mapUserUsage.put(customer_id, mapInfo);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	// private String generatedSQLUpdateUserUsageSingle(String table, String
	// customer_id,
	// Map<String, Integer> mapUsage, String date) {
	// String sql = "UPDATE " + table + " SET date = '" + date + "'";
	// for (int i = 0; i < 24; i++) {
	// String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX +
	// NumberUtils.get2CharNumber(i);
	// sql = sql + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX +
	// NumberUtils.get2CharNumber(i) + " = "
	// + mapUsage.get(key);
	// }
	// for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
	// String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
	// sql = sql + "," + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase() + "
	// = " + mapUsage.get(key);
	// }
	// sql = sql + " WHERE date = '" + date + "' AND customer_id = '" +
	// customer_id + "';";
	// return sql;
	// }

	private String generatedSQLUpdateUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage, String date) {
		String sql = "UPDATE now AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserUsage.get(customerId), date);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn()
				+ "WHERE c.customer_id = t.customer_id AND c.date::date = t.date;";
	}

	// private String generatedSQLInsertUserUsageSingle(String table, String
	// customer_id, Map<String, Integer> mapUsage,
	// String date) {
	// String sql = "INSERT INTO " + table + " " + generatedStringColumn() + "
	// VALUES "
	// + generatedStringValue(customer_id, mapUsage, date) + ";";
	// return sql;
	// }

	private String generatedSQLInsertUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage, String date) {
		String sql = "INSERT INTO now " + generatedStringColumn() + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			sql = sql + generatedStringValue(customerId, mapUserUsage.get(customerId), date) + ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private String generatedStringSetCommand() {
		String setCommand = "SET ";
		for (int i = 0; i < 24; i++) {
			setCommand = setCommand + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + " = c."
					+ PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + ",";
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			setCommand = setCommand + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase() + " = c."
					+ PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase() + ",";
		}
		return setCommand.substring(0, setCommand.length() - 1);
	}

	private String generatedStringValue(String customer_id, Map<String, Integer> mapUsage, String date) {
		String value = "('" + customer_id + "','" + date + "'";
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		value = value + ")";
		return value;
	}

	private String generatedStringColumn() {
		String col = "(customer_id,date";
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			col = col + "," + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
		}
		col = col + ")";
		return col;
	}

}
