package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class PostgreSQLPayTV {

	public static final String SQL_CREATE_TABLE_NOW = "CREATE TABLE now (contract VARCHAR(22), customer_id VARCHAR(22), date DATE, "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "PRIMARY KEY(customer_id, date));";

	public static String generatedSQLCreateTriggerDeleteOldRows(String table, String timeStampCol, int days) {
		String sql = "CREATE FUNCTION delete_old_rows() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN DELETE FROM "
				+ table + " WHERE " + timeStampCol + " < NOW() - INTERVAL '" + days + " days'; RETURN NULL; END; $$;";
		return sql;
	}

	public static String generatedSQLCallingTriggger(String table) {
		String sql = "CREATE TRIGGER trigger_delete_old_rows AFTER INSERT ON " + table
				+ " EXECUTE PROCEDURE delete_old_rows();";
		return sql;
	}

	public static String generatedSQLDeleteOldRows(String table, String timeStampCol, int days) {
		String sql = "DELETE FROM " + table + " WHERE " + timeStampCol + " < NOW() - INTERVAL '" + days + " days';";
		return sql;
	}

	public static void main(String[] args) {
		PostgreSQLPayTV postgreSQLPayTV = new PostgreSQLPayTV();
		postgreSQLPayTV.test();
	}

	public void test() {
		Connection connection = getConnection("localhost", 5432, "pay_tv", "tunn", "123456");
		executeSQL(connection, SQL_CREATE_TABLE_NOW);
		DateTime dateTime = new DateTime();
		String date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);

		Map<String, Map<String, Integer>> mapUserUsage = new HashMap<>();
		Map<String, String> mapUserContract = new HashMap<>();
		Map<String, Integer> mapInfo = new HashMap<>();
		for (int i = 0; i < 24; i++) {
			mapInfo.put(Integer.toString(i), 300);
		}
		for (String app : PayTVUtils.SET_APP_NAME_RTP) {
			mapInfo.put(app, 220000);
		}
		mapInfo.put("12", 622);
		mapInfo.put("IPTV", 5555);
		mapUserUsage.put("id1", mapInfo);
		mapUserContract.put("id1", "contest1");
		mapUserUsage.put("id2", mapInfo);
		mapUserContract.put("id2", "contest1");
		mapUserUsage.put("id3", mapInfo);
		mapUserContract.put("id3", "contest2");

		try {
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Connection getConnection(String host, int port, String db, String user, String password) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + db, user,
					password);
			connection.setAutoCommit(false);
			PayTVUtils.LOG_INFO.info("--- Open connection: " + "jdbc:postgresql://" + host + ":" + port + "/" + db);
		} catch (Exception e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
		}
		return connection;
	}

	public void executeSQL(Connection connection, String sql) {
		try {
			Statement statement = connection.createStatement();
			System.out.println("SQL: " + sql);
			statement.executeUpdate(sql);
			statement.close();
			connection.commit();
		} catch (SQLException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
		}
	}

	public long insertUserUsageMultiple(Connection connection, String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		long start = System.currentTimeMillis();
		try {
			Statement statement = connection.createStatement();
			String sql = generatedSQLInsertUserUsageMultiple(table, mapUserUsage, mapUserContract, date);
			// System.out.println(sql);
			statement.executeUpdate(sql);
			statement.close();
			connection.commit();
		} catch (SQLException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
		}
		return System.currentTimeMillis() - start;
	}
	
	public long updateUserUsageSingle(Connection connection, String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		long start = System.currentTimeMillis();
		try {
			Statement statement = connection.createStatement();
			for (String customerId : mapUserUsage.keySet()) {
				String sql = generatedSQLUpdateUserUsageSingle(table, customerId, mapUserUsage.get(customerId), date);
				statement.executeUpdate(sql);
			}
			statement.close();
			connection.commit();
		} catch (SQLException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
		}
		return System.currentTimeMillis() - start;
	}

	public long updateUserUsageMultiple(Connection connection, String table,
			Map<String, Map<String, Integer>> mapUserUsage, Map<String, String> mapUserContract, String date) {
		long start = System.currentTimeMillis();
		try {
			Statement statement = connection.createStatement();
			String sql = generatedSQLUpdateUserUsageMultiple(table, mapUserUsage, mapUserContract, date);
			// System.out.println(sql);
			statement.executeUpdate(sql);
			statement.close();
			connection.commit();
		} catch (SQLException e) {
			PayTVUtils.LOG_ERROR.error(e.getMessage());
		}
		return System.currentTimeMillis() - start;
	}

	public Map<String, Map<String, Integer>> queryUserUsage(Connection connection, String table, String date,
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
					mapInfo.put(Integer.toString(i), resultSet.getInt("h" + NumberUtils.getTwoCharNumber(i)));
				}
				for (String app : PayTVUtils.SET_APP_NAME_RTP) {
					mapInfo.put(app, resultSet.getInt("a_" + app.toLowerCase()));
				}
				mapUserUsage.put(customer_id, mapInfo);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapUserUsage;
	}

	private String generatedSQLUpdateUserUsageSingle(String table, String customer_id, Map<String, Integer> mapUsage,
			String date) {
		String sql = "UPDATE " + table + " SET date = '" + date + "'";
		for (int i = 0; i < 24; i++) {
			sql = sql + ", h" + NumberUtils.getTwoCharNumber(i) + " = " + mapUsage.get(Integer.toString(i));
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			sql = sql + ", a_" + app.toLowerCase() + " = " + mapUsage.get(app);
		}
		sql = sql + " WHERE date = '" + date + "' AND customer_id = '" + customer_id + "';";
		return sql;
	}

	private String generatedSQLUpdateUserUsageMultiple(String table, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String date) {
		String sql = "UPDATE " + table + " AS t " + getStringSetCommand() + " FROM (VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			String val = getStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId),
					date);
			sql = sql + val + ",";
		}
		return sql.substring(0, sql.length() - 1) + ") AS c" + getStringColumn()
				+ "WHERE c.customer_id = t.customer_id AND c.date::date = t.date;";
	}

	private String generatedSQLInsertUserUsageSingle(String table, String customer_id, String contract,
			Map<String, Integer> mapUsage, String date) {
		String sql = "INSERT INTO " + table + " " + getStringColumn() + " VALUES "
				+ getStringValue(customer_id, contract, mapUsage, date) + ";";
		return sql;
	}

	private String generatedSQLInsertUserUsageMultiple(String table, Map<String, Map<String, Integer>> mapUserUsage,
			Map<String, String> mapUserContract, String date) {
		String sql = "INSERT INTO " + table + " " + getStringColumn() + " VALUES ";
		for (String customerId : mapUserUsage.keySet()) {
			sql = sql + getStringValue(customerId, mapUserContract.get(customerId), mapUserUsage.get(customerId), date)
					+ ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private String getStringSetCommand() {
		String setCommand = "SET ";
		for (int i = 0; i < 24; i++) {
			setCommand = setCommand + "h" + NumberUtils.getTwoCharNumber(i) + " = c.h" + NumberUtils.getTwoCharNumber(i)
					+ ",";
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			setCommand = setCommand + "a_" + app.toLowerCase() + " = c.a_" + app.toLowerCase() + ",";
		}
		return setCommand.substring(0, setCommand.length() - 1);
	}

	private String getStringValue(String customer_id, String contract, Map<String, Integer> mapUsage, String date) {
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

	private String getStringColumn() {
		String col = "(contract,customer_id,date";
		for (int i = 0; i < 24; i++) {
			col = col + ",h" + NumberUtils.getTwoCharNumber(i);
		}
		for (String app : PayTVUtils.LIST_APP_NAME_RTP) {
			col = col + ",a_" + app;
		}
		col = col + ")";
		return col;
	}

}
