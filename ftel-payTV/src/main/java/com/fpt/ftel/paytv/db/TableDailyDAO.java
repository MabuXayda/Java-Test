package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.joda.time.DateTime;

import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableDailyDAO {
	private static final String SQL_CREATE_TABLE_DAILY = "CREATE TABLE IF NOT EXISTS daily (contract VARCHAR(22), customer_id VARCHAR(22), date DATE, "
			+ "h_00 INT, h_01 INT, h_02 INT, h_03 INT, h_04 INT, h_05 INT, h_06 INT, h_07 INT, h_08 INT, h_09 INT, "
			+ "h_10 INT, h_11 INT, h_12 INT, h_13 INT, h_14 INT, h_15 INT, h_16 INT, h_17 INT, h_18 INT, h_19 INT, "
			+ "h_20 INT, h_21 INT, h_22 INT, h_23 INT, "
			+ "a_iptv INT, a_vod INT, a_sport INT, a_child INT, a_relax INT, a_service INT, a_bhd INT, a_fims INT, "
			+ "PRIMARY KEY(customer_id, date));";

	public void createTable(Connection connection) throws SQLException {
		System.out.println(SQL_CREATE_TABLE_DAILY);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_DAILY);
		System.out.println(generatedSumFunction());
		PostgreSQL.executeUpdateSQL(connection, generatedSumFunction());
	}

	public void createPartition(Connection connection, String dateString) throws SQLException {
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
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO daily WHERE (date >= DATE "
				+ startDate + " AND date <= DATE " + endDate + ") DO INSTEAD INSERT INTO " + partition
				+ " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition + " (customer_id, date);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}

	public void dropPartition(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int week = date.getWeekOfWeekyear();
		String sqlDropPartition = "DROP TABLE IF EXISTS " + "daily_y" + year + "_w" + week + " CASCADE;";
		System.out.println(sqlDropPartition);
		PostgreSQL.executeUpdateSQL(connection, sqlDropPartition);

	}

	public void insertFromTable(Connection connection, String dateString) throws SQLException {
		String sql = "INSERT INTO daily SELECT * from now WHERE date = '" + dateString + "';";
		System.out.println(sql);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	private String generatedSumFunction() {
		String sql = "CREATE OR REPLACE FUNCTION sum(rec daily) RETURNS INT LANGUAGE SQL AS $$ SELECT ";
		for (int i = 0; i < 24; i++) {
			sql = sql + "$1." + PayTVDBUtils.VECTOR_HOURLY_PREFIX + NumberUtils.get2CharNumber(i) + " + ";
		}
		sql = sql.substring(0, sql.length() - 3);
		sql = sql + "; $$;";
		return sql;
	}

}
