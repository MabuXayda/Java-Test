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
import com.fpt.ftel.postgresql.ConnectionFactory;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableProfileDAO {
	public static void main(String[] args) throws SQLException {
		TableProfileDAO tableProfileDAO = new TableProfileDAO();
		Connection connection = ConnectionFactory.openConnection("localhost", 5432, "pay_tv", "tunn", "123456");
		String dateString = "2016-08-21";
		Map<String, Map<String, Integer>> mapUserusage = tableProfileDAO.queryUserUsageDaily(connection, dateString);
		System.out.println(mapUserusage.size());
		int tmp = 0;
		for (String id : mapUserusage.keySet()) {
			if (tmp > 10) {
				break;
			}
			System.out.print(id);
			Map<String, Integer> mapInfo = mapUserusage.get(id);
			for (String key : mapInfo.keySet()) {
				System.out.print("|" + key + ":" + mapInfo.get(key));
			}
			System.out.println();
			tmp++;
		}
	}

	private static final String SQL_CREATE_TABLE_PROFILE_SUM = "CREATE TABLE profile_sum (contract VARCHAR(22), customer_id VARCHAR(22), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "hourly_ml7 TEXT, daily_ml7 TEXT, app_ml7 TEXT, "
			+ "hourly_ml28 TEXT, daily_ml28 TEXT, app_ml28 TEXT, days_ml28 TEXT, " + "PRIMARY KEY(customer_id));";

	private static final String SQL_CREATE_TABLE_PROFILE_WEEK = "CREATE TABLE profile_week (contract VARCHAR(22), customer_id VARCHAR(22), week VARCHAR(10), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, week));";

	private static final String SQL_CREATE_TABLE_PROFILE_MONTH = "CREATE TABLE profile_month (contract VARCHAR(22), customer_id VARCHAR(22), month VARCHAR(10), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, month));";

	public void createTable(Connection connection) throws SQLException {
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_PROFILE_SUM);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_PROFILE_WEEK);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_PROFILE_MONTH);
	}

	public void dropPartitionWeek(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String sqlDropPartition = "DROP TABLE IF EXISTS " + "profile_week_y" + year + "_w" + week + " CASCADE;";
		System.out.println(sqlDropPartition);
		PostgreSQL.executeUpdateSQL(connection, sqlDropPartition);
	}

	public void dropPartitionMonth(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int month = date.getWeekOfWeekyear();
		String sqlDropPartition = "DROP TABLE IF EXISTS " + "profile_month_y" + year + "_m" + month + " CASCADE;";
		System.out.println(sqlDropPartition);
		PostgreSQL.executeUpdateSQL(connection, sqlDropPartition);
	}

	public void createPartitionWeek(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String partition = "profile_week_y" + year + "_w" + week;
		String partitionRule = "profile_week_insert_y" + year + "_w" + week;
		String partitionIndex = partition + "_index";
		String weekIndex = "y" + year + "_w" + week;
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (week = '" + weekIndex
				+ "')) INHERITS (profile_week);";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO profile_week WHERE (week = '"
				+ weekIndex + "') DO INSTEAD INSERT INTO " + partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition + " (customer_id, week);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public void createPartitionMonth(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int month = date.getMonthOfYear();
		String partition = "profile_month_y" + year + "_m" + month;
		String partitionRule = "profile_month_insert_y" + year + "_m" + month;
		String partitionIndex = partition + "_index";
		String monthIndex = "y" + year + "_m" + month;
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (month = '" + monthIndex
				+ "')) INHERITS (profile_month);";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO profile_month WHERE (month = '"
				+ monthIndex + "') DO INSTEAD INSERT INTO " + partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition
				+ " (customer_id, month);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public Map<String, String> queryUserContract(Connection connection, String dateString) throws SQLException {
		Map<String, String> result = new HashMap<>();
		Statement statement = connection.createStatement();
		String sqlConfig = "SET constraint_exclusion = on;";
		statement.execute(sqlConfig);
		String sqlSelect = "SELECT DISTINCT(customer_id), contract FROM daily WHERE date = '" + dateString + "';";
		ResultSet resultSet = statement.executeQuery(sqlSelect);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			String contract = resultSet.getString("contract");
			result.put(customer_id, contract);
		}
		resultSet.close();
		statement.close();
		return result;
	}

	public void updateUserUsageMultipleML(Connection connection, Map<String, Map<String, String>> mapUserUsage)
			throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultipleML(mapUserUsage);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract) throws SQLException {
		String sql = generatedSQLInsertUserUsageMultiple(mapUserUsage, mapUserContract);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract) throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultiple(mapUserUsage, mapUserContract);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String dateString, String type) throws SQLException {
		String sql = generatedSQLInsertUserUsageMultiple(mapUserUsage, mapUserContract, dateString, type);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String dateString, String type) throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultiple(mapUserUsage, mapUserContract, dateString, type);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public Map<String, Map<String, Integer>> queryUserUsageDaily(Connection connection, String dateString)
			throws SQLException {
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		Statement statement = connection.createStatement();
		String sqlConfig = "SET constraint_exclusion = on;";
		statement.execute(sqlConfig);
		String sqlSelect = "SELECT *, daily.sum FROM daily WHERE date = '" + dateString + "';";
		ResultSet resultSet = statement.executeQuery(sqlSelect);
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
			mapInfo.put("sum", resultSet.getInt("sum"));
			mapUserUsage.put(customer_id, mapInfo);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	public Map<String, Map<String, Integer>> queryUserUsageDaily(Connection connection, String dateString,
			Set<String> setUser) throws SQLException {
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		Statement statement = connection.createStatement();
		String sqlConfig = "SET constraint_exclusion = on;";
		statement.execute(sqlConfig);
		String sqlSelect = "SELECT *, daily.sum FROM daily WHERE date = '" + dateString + "' AND customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		ResultSet resultSet = statement.executeQuery(sqlSelect);
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
			mapInfo.put("sum", resultSet.getInt("sum"));
			mapUserUsage.put(customer_id, mapInfo);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	public Map<String, Map<String, Integer>> queryUserUsageWeek(Connection connection, Set<String> setUser,
			String dateString) throws SQLException {
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		String weekIndex = "y" + date.getYear() + "_w" + date.getWeekOfWeekyear();
		Statement statement = connection.createStatement();
		String sqlConfig = "SET constraint_exclusion = on;";
		statement.execute(sqlConfig);
		String sql = "SELECT * FROM profile_week WHERE week = '" + weekIndex + "' AND customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		ResultSet resultSet = statement.executeQuery(sql);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			Map<String, Integer> mapUsage = new HashMap<>();
			for (int i = 0; i < 24; i++) {
				String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
				mapUsage.put(key, resultSet.getInt(key));
			}
			for (String app : PayTVUtils.SET_APP_NAME_RTP) {
				String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
				mapUsage.put(key, resultSet.getInt(key));
			}
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
				mapUsage.put(key, resultSet.getInt(key));
			}
			mapUserUsage.put(customer_id, mapUsage);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	public Map<String, Map<String, Integer>> queryUserUsageMonth(Connection connection, Set<String> setUser,
			String dateString) throws SQLException {
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		String monthIndex = "y" + date.getYear() + "_m" + date.getMonthOfYear();
		Statement statement = connection.createStatement();
		String sqlConfig = "SET constraint_exclusion = on;";
		statement.execute(sqlConfig);
		String sql = "SELECT * FROM profile_month WHERE month = '" + monthIndex + "' AND customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		ResultSet resultSet = statement.executeQuery(sql);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			Map<String, Integer> mapUsage = new HashMap<>();
			for (int i = 0; i < 24; i++) {
				String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
				mapUsage.put(key, resultSet.getInt(key));
			}
			for (String app : PayTVUtils.SET_APP_NAME_RTP) {
				String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
				mapUsage.put(key, resultSet.getInt(key));
			}
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
				mapUsage.put(key, resultSet.getInt(key));
			}
			mapUserUsage.put(customer_id, mapUsage);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	public Map<String, Map<String, Integer>> queryUserUsageSum(Connection connection, Set<String> setUser)
			throws SQLException {
		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		String sql = "SELECT customer_id," + generatedColSelectHourly() + "," + generatedColSelectApp() + ","
				+ generatedColSelectDaily() + " FROM profile_sum WHERE customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			Map<String, Integer> mapUsage = new HashMap<>();
			for (int i = 0; i < 24; i++) {
				String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
				mapUsage.put(key, resultSet.getInt(key));
			}
			for (String app : PayTVUtils.SET_APP_NAME_RTP) {
				String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
				mapUsage.put(key, resultSet.getInt(key));
			}
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
				mapUsage.put(key, resultSet.getInt(key));
			}
			mapUserUsage.put(customer_id, mapUsage);
		}
		resultSet.close();
		statement.close();
		return mapUserUsage;
	}

	private String generatedColSelectHourly() {
		String result = "";
		for (int i = 0; i < 24; i++) {
			result = result + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		return result.substring(1);
	}

	private String generatedColSelectApp() {
		String result = "";
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			result = result + "," + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
		}
		return result.substring(1);
	}

	private String generatedColSelectDaily() {
		String result = "";
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			result = result + "," + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
		}
		return result.substring(1);
	}

	public Map<String, Map<String, String>> queryUserUsageML(Connection connection) throws SQLException {
		Map<String, Map<String, String>> mapUserUsageML = new HashMap<>();
		String col = "hourly" + PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX + ",app" + PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX
				+ ",daily" + PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX + ",hourly"
				+ PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX + ",app" + PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX + ",daily"
				+ PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX + ",days" + PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
		String sql = "SELECT customer_id," + col + " FROM profile_sum;";
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);
		while (resultSet.next()) {
			String customer_id = resultSet.getString("customer_id");
			Map<String, String> mapUsageML = new HashMap<>();
			String key = "hourly" + PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			key = "app" + PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			key = "daily" + PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			key = "hourly" + PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			key = "app" + PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			key = "daily" + PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			key = "days" + PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
			mapUsageML.put(key, resultSet.getString(key) == null ? "" : resultSet.getString(key));
			mapUserUsageML.put(customer_id, mapUsageML);
		}
		resultSet.close();
		statement.close();
		return mapUserUsageML;
	}

	private String generatedSQLUpdateUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String dateString, String type) {
		String sql = "UPDATE profile_" + type + " AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId),
					dateString, type);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn(type)
				+ "WHERE c.customer_id = t.customer_id AND c." + type + " = t." + type + ";";
	}

	private String generatedSQLInsertUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String dateString, String type) {
		String sql = "INSERT INTO profile_" + type + " " + generatedStringColumn(type) + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			sql = sql + generatedStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId),
					dateString, type) + ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private String generatedStringValue(String customer_id, String contract, Map<String, Integer> mapUsage,
			String dateString, String type) {
		String typeIndex = null;
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		if (type.equals("week")) {
			typeIndex = "y" + date.getYear() + "_w" + date.getWeekOfWeekyear();
		} else if (type.equals("month")) {
			typeIndex = "y" + date.getYear() + "_m" + date.getMonthOfYear();
		}

		String value = "('" + contract + "','" + customer_id + "','" + typeIndex + "'";
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		value = value + ")";
		return value;
	}

	private String generatedStringColumn(String type) {
		String col = "(contract,customer_id," + type;
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			col = col + "," + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			col = col + "," + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
		}
		col = col + ")";
		return col;
	}

	private String generatedSQLUpdateUserUsageMultipleML(Map<String, Map<String, String>> mapUserUsageML) {
		String sql = "UPDATE profile_sum AS t " + generatedStringSetCommandML() + " FROM (VALUES ";
		for (String customerId : mapUserUsageML.keySet()) {
			String val = generatedStringValueMLUpdate(customerId, mapUserUsageML.get(customerId));
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumnMLUpdate()
				+ "WHERE c.customer_id = t.customer_id;";
	}

	private String generatedStringValueMLUpdate(String customer_id, Map<String, String> mapUsageML) {
		String value = "('" + customer_id + "','" + mapUsageML.get("hourly_ml7") + "','" + mapUsageML.get("app_ml7")
				+ "','" + mapUsageML.get("daily_ml7") + "','" + mapUsageML.get("hourly_ml28") + "','"
				+ mapUsageML.get("app_ml28") + "','" + mapUsageML.get("daily_ml28") + "','"
				+ mapUsageML.get("days_ml28") + "')";
		return value;
	}

	private String generatedStringColumnMLUpdate() {
		String col = "(customer_id, hourly_ml7, app_ml7, daily_ml7, hourly_ml28, app_ml28, daily_ml28, days_ml28)";
		return col;
	}

	private String generatedStringSetCommandML() {
		String setCommand = "SET hourly_ml7 = c.hourly_ml7, app_ml7 = c.app_ml7, daily_ml7 = c.daily_ml7, "
				+ "hourly_ml28 = c.hourly_ml28, app_ml28 = c.app_ml28, daily_ml28 = c.daily_ml28, days_ml28 = c.days_ml28";
		return setCommand;
	}

	private String generatedSQLUpdateUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract) {
		String sql = "UPDATE profile_sum AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserContract.get(customerId),
					mapUserUsage.get(customerId));
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn()
				+ "WHERE c.customer_id = t.customer_id;";
	}

	private String generatedSQLInsertUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract) {
		String sql = "INSERT INTO profile_sum " + generatedStringColumnMLInsert() + "" + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			sql = sql + generatedStringValueMLInsert(customerId, mapUserContract.get(customerId),
					mapUserUsage.get(customerId)) + ",";
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
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			setCommand = setCommand + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase() + " = c."
					+ PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase() + ",";
		}
		return setCommand.substring(0, setCommand.length() - 1);
	}

	private String generatedStringValue(String customer_id, String contract, Map<String, Integer> mapUsage) {
		String value = "('" + contract + "','" + customer_id + "'";
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
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
		String col = "(contract,customer_id";
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			col = col + "," + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			col = col + "," + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
		}
		col = col + ")";
		return col;
	}

	private String generatedStringColumnMLInsert() {
		String col = "(contract,customer_id";
		for (int i = 0; i < 24; i++) {
			col = col + "," + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			col = col + "," + PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			col = col + "," + PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
		}
		col = col + ", hourly_ml7, app_ml7, daily_ml7, hourly_ml28, app_ml28, daily_ml28, days_ml28)";
		return col;
	}

	private String generatedStringValueMLInsert(String customer_id, String contract, Map<String, Integer> mapUsage) {
		String value = "('" + contract + "','" + customer_id + "'";
		for (int i = 0; i < 24; i++) {
			String key = PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i);
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			String key = PayTVDBUtils.VECTOR_APP_PREFIX + app.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
			String key = PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase();
			value = value + "," + (mapUsage.get(key) == null ? 0 : mapUsage.get(key));
		}
		value = value + ",'" + PayTVDBUtils.generatedJsonEmptyVectorHourly() + "','"
				+ PayTVDBUtils.generatedJsonEmptyVectorApp() + "','" + PayTVDBUtils.generatedJsonEmptyVectorDaily()
				+ "','" + PayTVDBUtils.generatedJsonEmptyVectorHourly() + "','"
				+ PayTVDBUtils.generatedJsonEmptyVectorApp() + "','" + PayTVDBUtils.generatedJsonEmptyVectorDaily()
				+ "','" + PayTVDBUtils.generatedJsonEmptyVectorDays() + "')";
		return value;
	}

}
