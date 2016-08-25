package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class SQLTableDaily {
	private static final String SQL_CREATE_TABLE_DAILY = "CREATE TABLE daily (contract VARCHAR(22), customer_id VARCHAR(22), date DATE, "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "PRIMARY KEY(customer_id, date));";

	public static void main(String[] args) {
		List<String> list = new ArrayList<>();
		boolean tmp = true;
		for(String str : list){
			if("123".equals(str + "123")){
				tmp = false;
				break;
			}
		}
		System.out.println(tmp);
	}

	public static void createTable(Connection connection, String table) {
		try {
			System.out.println(SQL_CREATE_TABLE_DAILY);
			PostgreSQL.executeSQL(connection, SQL_CREATE_TABLE_DAILY);
			System.out.println(generatedSumFunction());
			PostgreSQL.executeSQL(connection, generatedSumFunction());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String generatedSumFunction() {
		String sql = "CREATE OR REPLACE FUNCTION sum(rec daily) RETURNS INT LANGUAGE SQL AS $$ SELECT ";
		for (int i = 0; i < 24; i++) {
			sql = sql + "$1." + PayTVDBParam.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + " + ";
		}
		sql = sql.substring(0, sql.length() - 3);
		sql = sql + "; $$;";
		return sql;
	}

	public static void createPartition(Connection connection, String dateString) {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String partition = "daily_y" + year + "_w" + week;
		String partitionRule = "daily_insert_y" + year + "_w" + week;
		String partitionIndex = partition + "_index";
		String startDate = "'"
				+ PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(new DateTime().withWeekOfWeekyear(week).withDayOfWeek(1))
				+ "'";
		String endDate = "'"
				+ PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(new DateTime().withWeekOfWeekyear(week).withDayOfWeek(7))
				+ "'";
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (date >= DATE " + startDate
				+ " AND date <= DATE " + endDate + ")) INHERITS (daily);";
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO daily WHERE (date >= DATE "
				+ startDate + " AND date <= DATE " + endDate + ") DO INSTEAD INSERT INTO " + partition
				+ " VALUES (NEW.*);";
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition + " (customer_id, date);";
		try {
			System.out.println(sqlCreate);
			PostgreSQL.executeSQL(connection, sqlCreate);
			System.out.println(sqlRule);
			PostgreSQL.executeSQL(connection, sqlRule);
			System.out.println(sqlIndex);
			PostgreSQL.executeSQL(connection, sqlIndex);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void dropPartition(Connection connection, String dateString){
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String partition = "daily_y" + year + "_w" + week;
//		String partitionRule = "daily_insert_y" + year + "_w" + week;
		String sqlDropPartition = "DROP TABLE IF EXISTS " + partition + " CASCADE;";
//		String sqlDropRule = "DROP RULE IF EXISTS " + partitionRule + " ON daily";
		try {
			PostgreSQL.executeSQL(connection, sqlDropPartition);
//			PostgreSQL.executeSQL(connection, sqlDropRule);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void insertFromTable(Connection connection, String dateString) {
		String sql = "INSERT INTO daily SELECT * from now WHERE date = '" + dateString + "';";
		System.out.println(sql);
		try {
			PostgreSQL.executeSQL(connection, sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
