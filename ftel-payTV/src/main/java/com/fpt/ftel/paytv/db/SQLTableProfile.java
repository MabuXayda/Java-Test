package com.fpt.ftel.paytv.db;

public class SQLTableProfile {
	private static final String SQL_CREATE_TABLE_PROFILE_SUM = "CREATE TABLE profile_sum (contract VARCHAR(22), customer_id VARCHAR(22), "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wes INT, d_tue INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "hourly_ml7 TEXT, daily_ml7 TEXT, app_ml7"
			+ "hourly_ml28 TEXT, daily_ml28 TEXT, app_ml28"
			+ "PRIMARY KEY(customer_id));";
	
	private static final String SQL_CREATE_TABLE_PROFILE_WEEK = "CREATE TABLE profile_week (contract VARCHAR(22), customer_id VARCHAR(22), week INT"
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wes INT, d_tue INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, week));";
	
	private static final String SQL_CREATE_TABLE_PROFILE_MONTH = "CREATE TABLE profile_month (contract VARCHAR(22), customer_id VARCHAR(22), month INT"
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "d_mon INT, d_tue INT, d_wes INT, d_tue INT, d_fri INT, d_sat INT, d_sun INT, "
			+ "PRIMARY KEY(customer_id, month));";
	
	
}
