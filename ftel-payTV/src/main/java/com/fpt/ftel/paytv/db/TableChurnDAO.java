package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.fpt.ftel.postgresql.PostgreSQL;

public class TableChurnDAO {
	private static final String SQL_CREATE_TABLE_CHURN_DAILY = "CREATE TABLE churn_daily (contract VARCHAR(22), customer_id VARCHAR(22), date DATE, "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "PRIMARY KEY(customer_id, date));";

	private static final String SQL_CREATE_TABLE_CHURN_PROFILE_SUM = "CREATE TABLE churn_profile_sum (contract VARCHAR(22), customer_id VARCHAR(22), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "hourly_ml7 TEXT, daily_ml7 TEXT, app_ml7 TEXT, "
			+ "hourly_ml28 TEXT, daily_ml28 TEXT, app_ml28 TEXT, days_ml28 TEXT, " + "PRIMARY KEY(customer_id));";

	private static final String SQL_CREATE_TABLE_CHURN_PROFILE_WEEK = "CREATE TABLE churn_profile_week (contract VARCHAR(22), customer_id VARCHAR(22), week VARCHAR(10), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, week));";

	private static final String SQL_CREATE_TABLE_CHURN_PROFILE_MONTH = "CREATE TABLE churn_profile_month (contract VARCHAR(22), customer_id VARCHAR(22), month VARCHAR(10), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wed INT, d_thu INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, month));";

	public void createTable(Connection connection) throws SQLException {
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_CHURN_DAILY);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_CHURN_PROFILE_SUM);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_CHURN_PROFILE_WEEK);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_CHURN_PROFILE_MONTH);
	}
	
	public void insertChurnDaily(Connection connection, Set<String> setId) throws SQLException {
		String sql = "INSERT INTO churn_daily SELECT * from daily WHERE customer_id IN ('"
				+ StringUtils.join(setId, "','") + "');";
		System.out.println(sql);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertChurnProfileSum(Connection connection, Set<String> setId) throws SQLException {
		String sql = "INSERT INTO churn_profile_sum SELECT * from profile_sum WHERE customer_id IN ('"
				+ StringUtils.join(setId, "','") + "');";
		System.out.println(sql);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertChurnProfileWeek(Connection connection, Set<String> setId) throws SQLException {
		String sql = "INSERT INTO churn_profile_week SELECT * from profile_week WHERE customer_id IN ('"
				+ StringUtils.join(setId, "','") + "');";
		System.out.println(sql);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertChurnProfileMonth(Connection connection, Set<String> setId) throws SQLException {
		String sql = "INSERT INTO churn_profile_month SELECT * from profile_month WHERE customer_id IN ('"
				+ StringUtils.join(setId, "','") + "');";
		System.out.println(sql);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}
	
	
}
