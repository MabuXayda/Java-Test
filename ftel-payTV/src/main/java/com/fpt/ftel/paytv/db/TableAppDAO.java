package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableAppDAO {
	private static final String SQL_CREATE_TABLE_APP = "CREATE TABLE IF NOT EXISTS profile_app (contract VARCHAR(22), customer_id VARCHAR(22), app VARCHAR(22), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, app));";

	public void createTable(Connection connection) throws SQLException {
		System.out.println(SQL_CREATE_TABLE_APP);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_APP);
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			createPartitionApp(connection, app.toLowerCase());
		}
	}

	public void createPartitionApp(Connection connection, String app) throws SQLException {
		String partition = "profile_app_" + app;
		String partitionRule = "profile_app_insert_" + app;
		String partitionIndex = partition + "_index";
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (app = '" + app
				+ "')) INHERITS (profile_app);";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO profile_app WHERE (app = '" + app
				+ "') DO INSTEAD INSERT INTO " + partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		// String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + "
		// ON " + partition + " (customer_id, app);";
		// System.out.println(sqlIndex);
		// PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String app) throws SQLException {
		String sql = generatedSQLInsertUserUsageMultiple(mapUserUsage, mapUserContract, app);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String app) throws SQLException {
		String sql = generatedSQLUpdateUserUsageMultiple(mapUserUsage, mapUserContract, app);
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

	private String generatedSQLUpdateUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String app) {
		String sql = "UPDATE profile_app AS t " + generatedStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = generatedStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId),
					app);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + generatedStringColumn()
				+ "WHERE c.customer_id = t.customer_id AND c.app = t.app;";
	}

	private String generatedSQLInsertUserUsageMultiple(Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String app) {
		String sql = "INSERT INTO profile_app " + generatedStringColumn() + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {

			if (mapUserUsage.get(customerId) != null) {
				sql = sql + generatedStringValue(customerId, mapUserContract.get(customerId),
						mapUserUsage.get(customerId), app) + ",";
			}
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

	private String generatedStringValue(String customer_id, String contract, Map<String, Integer> mapUsage,
			String app) {
		String value = "('" + contract + "','" + customer_id + "','" + app + "'";
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
		String col = "(contract,customer_id,app";
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
