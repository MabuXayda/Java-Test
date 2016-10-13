package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableAppDAO {
	private static final String SQL_CREATE_TABLE_PROFILE_APP = "CREATE TABLE IF NOT EXISTS profile_app "
			+ "(customer_id VARCHAR(22), app VARCHAR(22), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, app));";

	private static final String SQL_CREATE_TABLE_PROFILE_APP_WEEK = "CREATE TABLE IF NOT EXISTS profile_app_week "
			+ "(customer_id VARCHAR(22), app VARCHAR(22), week VARCHAR(10), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, app, week));";

	private static final String SQL_CREATE_TABLE_PROFILE_APP_MONTH = "CREATE TABLE IF NOT EXISTS profile_app_month "
			+ "(customer_id VARCHAR(22), app VARCHAR(22), month VARCHAR(10), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, app, month));";

	public void createTable(Connection connection) throws SQLException {
		System.out.println(SQL_CREATE_TABLE_PROFILE_APP);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_PROFILE_APP);
		System.out.println(SQL_CREATE_TABLE_PROFILE_APP_WEEK);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_PROFILE_APP_WEEK);
		System.out.println(SQL_CREATE_TABLE_PROFILE_APP_MONTH);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_PROFILE_APP_MONTH);
//		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
//			createPartitionApp(connection, app);
//		}
	}

	public void createPartitionApp(Connection connection, String app) throws SQLException {
		String partition = "profile_app_" + app.toLowerCase();
		String partitionRule = "profile_app_insert_" + app.toLowerCase();
		String partitionIndex = partition + "_index";
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (app = '" + app
				+ "')) INHERITS (profile_app);";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO profile_app WHERE (app = '" + app
				+ "') DO INSTEAD INSERT INTO " + partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition + " (customer_id, app);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public void dropPartitionWeek(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String sqlDropPartition = "DROP TABLE IF EXISTS " + "profile_app_week_y" + year + "_w" + week + " CASCADE;";
		System.out.println(sqlDropPartition);
		PostgreSQL.executeUpdateSQL(connection, sqlDropPartition);
	}

	public void dropPartitionMonth(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int month = date.getWeekOfWeekyear();
		String sqlDropPartition = "DROP TABLE IF EXISTS " + "profile_app_month_y" + year + "_m" + month + " CASCADE;";
		System.out.println(sqlDropPartition);
		PostgreSQL.executeUpdateSQL(connection, sqlDropPartition);
	}

	public void createPartitionWeek(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String partition = "profile_app_week_y" + year + "_w" + week;
		String partitionRule = "profile_app_week_insert_y" + year + "_w" + week;
		String partitionIndex = partition + "_index";
		String weekIndex = "y" + year + "_w" + week;
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (week = '" + weekIndex
				+ "')) INHERITS (profile_app_week);";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO profile_app_week WHERE (week = '"
				+ weekIndex + "') DO INSTEAD INSERT INTO " + partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition
				+ " (customer_id, app, week);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public void createPartitionMonth(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int month = date.getMonthOfYear();
		String partition = "profile_app_month_y" + year + "_m" + month;
		String partitionRule = "profile_app_month_insert_y" + year + "_m" + month;
		String partitionIndex = partition + "_index";
		String monthIndex = "y" + year + "_m" + month;
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (month = '" + monthIndex
				+ "')) INHERITS (profile_app_month);";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule
				+ " AS ON INSERT TO profile_app_month WHERE (month = '" + monthIndex + "') DO INSTEAD INSERT INTO "
				+ partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition
				+ " (customer_id, app, month);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public void updateUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			String app) throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultiple(mapUserUsage, app);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			String app, String dateString, String type) throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultiple(mapUserUsage, app, dateString, type);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			String app) throws SQLException {
		String sql = generatedSQLInsertUserUsageMultiple(mapUserUsage, app);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			String app, String dateString, String type) throws SQLException {
		String sql = generatedSQLInsertUserUsageMultiple(mapUserUsage, app, dateString, type);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public Map<String, Map<String, Integer>> queryUserUsage(Connection connection, String app, Set<String> setUser)
			throws SQLException {
		PostgreSQL.setConstraintExclusion(connection, true);
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		String sql = "SELECT * FROM profile_app WHERE app = '" + app + "' AND customer_id IN ('"
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
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
				mapInfo.put(key, resultSet.getInt(key));
			}
			mapUserUsage.put(customer_id, mapInfo);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	public Map<String, Map<String, Integer>> queryUserUsage(Connection connection, String app, Set<String> setUser,
			String dateString, String type) throws SQLException {
		PostgreSQL.setConstraintExclusion(connection, true);
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		String sql = null;
		if (type.equals("week")) {
			String weekIndex = "y" + date.getYear() + "_w" + date.getWeekOfWeekyear();
			sql = "SELECT * FROM profile_app_week WHERE week = '" + weekIndex + "' AND app = '" + app
					+ "' AND customer_id IN ('" + StringUtils.join(setUser, "','") + "');";
		} else if (type.equals("month")) {
			String monthIndex = "y" + date.getYear() + "_m" + date.getMonthOfYear();
			sql = "SELECT * FROM profile_app_month WHERE month = '" + monthIndex + "' AND app = '" + app
					+ "' AND customer_id IN ('" + StringUtils.join(setUser, "','") + "');";
		}
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			Map<String, Integer> mapInfo = new HashMap<>();
			for (int i = 0; i < 24; i++) {
				String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
				mapInfo.put(key, resultSet.getInt(key));
			}
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
				mapInfo.put(key, resultSet.getInt(key));
			}
			mapUserUsage.put(customer_id, mapInfo);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	private String generatedSQLUpdateUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			String app) {
		String sql = "UPDATE profile_app AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserUsage.get(customerId),
					app);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn()
				+ "WHERE c.customer_id = t.customer_id AND c.app = t.app;";
	}

	private String generatedSQLUpdateUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			String app, String dateString, String type) {
		String sql = "UPDATE profile_app_" + type + " AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserUsage.get(customerId),
					app, dateString, type);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn(type)
				+ "WHERE c.customer_id = t.customer_id AND c.app = t.app AND c." + type + " = t." + type + ";";
	}

	private String generatedSQLInsertUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			 String app) {
		String sql = "INSERT INTO profile_app " + generatedStringColumn() + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {

			if (mapUserUsage.get(customerId) != null) {
				sql = sql + generatedStringValue(customerId, 
						mapUserUsage.get(customerId), app) + ",";
			}
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private String generatedSQLInsertUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			String app, String dateString, String type) {
		String sql = "INSERT INTO profile_app_" + type + " " + generatedStringColumn(type) + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			sql = sql + generatedStringValue(customerId, mapUserUsage.get(customerId),
					app, dateString, type) + ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private String generatedStringSetCommand() {
		String setCommand = "SET ";
		for (int i = 0; i < 24; i++) {
			setCommand = setCommand + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + "=c."
					+ PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + ",";
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			setCommand = setCommand + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase() + "=c."
					+ PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase() + ",";
		}
		return setCommand.substring(0, setCommand.length() - 1);
	}

	private String generatedStringValue(String customer_id, Map<String, Integer> mapUsage,
			String app) {
		String value = "('" + customer_id + "','" + app + "'";
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		value = value + ")";
		return value;
	}

	private String generatedStringValue(String customer_id, Map<String, Integer> mapUsage, String app,
			String dateString, String type) {
		String typeIndex = null;
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		if (type.equals("week")) {
			typeIndex = "y" + date.getYear() + "_w" + date.getWeekOfWeekyear();
		} else if (type.equals("month")) {
			typeIndex = "y" + date.getYear() + "_m" + date.getMonthOfYear();
		}

		String value = "('" + customer_id + "','" + app + "','" + typeIndex + "'";
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		value = value + ")";
		return value;
	}

	private String generatedStringColumn() {
		String col = "(customer_id,app";
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			col = col + "," + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
		}
		col = col + ")";
		return col;
	}

	private String generatedStringColumn(String type) {
		String col = "(customer_id,app," + type;
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			col = col + "," + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
		}
		col = col + ")";
		return col;
	}

}
