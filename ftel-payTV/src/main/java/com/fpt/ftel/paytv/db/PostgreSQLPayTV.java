package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class PostgreSQLPayTV {
	public static final String SQL_CREATE_TABLE_NOW = "CREATE TABLE now (contract VARCHAR(22), customer_id VARCHAR(22), date DATE, "
			+ "h00 INT, h01 INT, h02 INT, h03 INT, h04 INT, h05 INT, h06 INT, h07 INT, h08 INT, h09 INT, "
			+ "h10 INT, h11 INT, h12 INT, h13 INT, h14 INT, h15 INT, h16 INT, h17 INT, h18 INT, h19 INT, "
			+ "h20 INT, h21 INT, h22 INT, h23 INT, "
			+ "a_IPTV INT, a_VOD INT, a_SPORT INT, a_CHILD INT, a_RELAX INT, a_SERVICE INT, a_BHD INT, a_FIMs INT, "
			+ "PRIMARY KEY(customer_id, date));";

	public static void main(String[] args) {
		PostgreSQLPayTV postgreSQLPayTV = new PostgreSQLPayTV();
		postgreSQLPayTV.process();
	}

	public void process() {
		Connection connection = getConnection("localhost", 5432, "pay_tv", "tunn", "123456");
		// executeSQL(connection, SQL_CREATE_TABLE_NOW);
		DateTime dateTime = new DateTime();
		String date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);

		Map<String, Map<String, Integer>> mapUserInfo = new HashMap<>();
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
		mapUserInfo.put("id1", mapInfo);
		mapUserContract.put("id1", "contest1");
		mapUserInfo.put("id2", mapInfo);
		mapUserContract.put("id2", "contest1");
		mapUserInfo.put("id3", mapInfo);
		mapUserContract.put("id3", "contest2");
		updateAllUser(connection, "now", date, mapUserInfo);
		// insertUserInfo(connection, "now", date, mapUserInfo,
		// mapUserContract);
//		Set<String> setUser = new HashSet<>();
//		setUser.addAll(mapUserInfo.keySet());
//		setUser.add("saokodc");
//		Map<String, Map<String, Integer>> result = queryUserInfo(connection, "now", date, setUser);
//		for (String id : result.keySet()) {
//			System.out.println(id);
//			for (String key : result.get(id).keySet()) {
//				System.out.print("," + key + "|" + result.get(id).get(key));
//			}
//			System.out.println();
//		}

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
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		return connection;
	}

	public void executeSQL(Connection connection, String sql) {
		try {
			Statement statement = connection.createStatement();
			statement.executeUpdate(sql);
			System.out.println("SQL executed: " + sql);
			statement.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insertUserInfo(Connection connection, String table, String date,
			Map<String, Map<String, Integer>> mapUserInfo, Map<String, String> mapUserContract) {
		try {
			Statement statement = connection.createStatement();
			String col = "customer_id,contract,date";
			for (int i = 0; i < 24; i++) {
				col = col + ",h" + NumberUtils.getTwoCharNumber(i);
			}
			List<String> listApp = new ArrayList<>(PayTVUtils.SET_APP_NAME_RTP);
			for (String app : listApp) {
				col = col + ",a_" + app;
			}
			for (String customerId : mapUserInfo.keySet()) {
				Map<String, Integer> mapInfo = mapUserInfo.get(customerId);
				String value = "'" + customerId + "','" + mapUserContract.get(customerId) + "','" + date + "'";
				for (int i = 0; i < 24; i++) {
					value = value + "," + mapInfo.get(Integer.toString(i));
				}
				for (String app : listApp) {
					value = value + "," + mapInfo.get(app);
				}
				String sql = "INSERT INTO " + table + " (" + col + ") VALUES (" + value + ");";
				System.out.println(sql);
				statement.executeUpdate(sql);
			}
			statement.close();
			// connection.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void updateOneUser(Connection connection, String table, String date,
			Map<String, Map<String, Integer>> mapUserInfo) {
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			for (String customerId : mapUserInfo.keySet()) {
				String sql = generatedSQLUpdateOneUser(table, customerId, mapUserInfo.get(customerId), date);
				System.out.println(sql);
				statement.executeUpdate(sql);
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			statement.close();
			connection.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void updateAllUser(Connection connection, String table, String date,
			Map<String, Map<String, Integer>> mapUserInfo) {
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			String sql = generatedSQLUpdateAllUser(table, mapUserInfo, date);
			System.out.println(sql);
			statement.executeUpdate(sql);
			statement.close();
			connection.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String generatedSQLUpdateOneUser(String table, String customer_id, Map<String, Integer> mapInfo,
			String date) {
		String sql = "UPDATE " + table + " SET date = '" + date + "'";
		for (int i = 0; i < 24; i++) {
			sql = sql + ", h" + NumberUtils.getTwoCharNumber(i) + " = " + mapInfo.get(Integer.toString(i));
		}
		List<String> listApp = new ArrayList<>(PayTVUtils.SET_APP_NAME_RTP);
		for (String app : listApp) {
			sql = sql + ", a_" + app.toLowerCase() + " = " + mapInfo.get(app);
		}
		sql = sql + " WHERE date = '" + date + "' AND customer_id = '" + customer_id + "';";
		return sql;
	}
	
	public String generatedSQLUpdateAllUser(String table, Map<String,Map<String, Integer>> mapUserInfo,
			String date) {
		List<String> listApp = new ArrayList<>(PayTVUtils.SET_APP_NAME_RTP);
		String sql = "UPDATE " + table + " AS t SET ";
		for (int i = 0; i < 24; i++) {
			sql = sql + "h" + NumberUtils.getTwoCharNumber(i) + " = c.h" + NumberUtils.getTwoCharNumber(i) + ",";
		}
		for (String app : listApp) {
			sql = sql + "a_" + app.toLowerCase() + " = c.a_" + app.toLowerCase() + ",";
		}
		sql = sql.substring(0, sql.length()-1) ;
		sql = sql + " FROM ( VALUES ";
		for(String customerId : mapUserInfo.keySet()){
			Map<String, Integer> mapInfo = mapUserInfo.get(customerId);
			String tmp = "('" + customerId + "','" + date +"'";
			for (int i = 0; i < 24; i++) {
				tmp = tmp + "," +  mapInfo.get(Integer.toString(i));
			}
			for (String app : listApp) {
				tmp = tmp + "," + mapInfo.get(app);
			}
			tmp = tmp + ")";
			sql = sql + tmp + ",";
		}
		sql = sql.substring(0, sql.length()-1) ;
		sql = sql + ") AS c(customer_id, date";
		for (int i = 0; i < 24; i++) {
			sql = sql + ",h" + NumberUtils.getTwoCharNumber(i); 
		}
		for (String app : listApp) {
			sql = sql + ",a_" + app.toLowerCase(); 
		}
		sql = sql + ") WHERE c.customer_id = t.customer_id AND c.date::date = t.date;";
		return sql;
	}

	public Map<String, Map<String, Integer>> queryUserInfo(Connection connection, String table, String date,
			Set<String> setUser) {
		Map<String, Map<String, Integer>> mapUserInfo = new HashMap<>();
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
				mapUserInfo.put(customer_id, mapInfo);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapUserInfo;
	}
}
