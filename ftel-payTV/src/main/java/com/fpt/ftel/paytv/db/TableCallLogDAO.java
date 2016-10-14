package com.fpt.ftel.paytv.db;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

public class TableCallLogDAO {
	private static final String TABLE_CALL_LOG = "call_log";
	private static final String COUNT = "cnt_";
	private static final String DURATION = "dur_";

	public static Map<String, String> getMapCallLogPurpose() {
		try {
			return new Gson().fromJson(
					new JsonReader(new FileReader("/home/tunn/data/tv/data_support/call_log_purpose.json")),
					new HashMap<>().getClass());
		} catch (JsonIOException | JsonSyntaxException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		TableCallLogDAO object = new TableCallLogDAO();
		System.out.println(object.getSQLCreateTableCallLog());
	}

	public String getSQLCreateTableCallLog() {
		Map<String, String> mapPurpose = getMapCallLogPurpose();
		String sql = "CREATE TABLE IF NOT EXISTS " + TABLE_CALL_LOG + " (contract VARCHAR(22), month VARCHAR(10), ";
		for (String purpose : mapPurpose.keySet()) {
			sql = sql + COUNT + mapPurpose.get(purpose) + " INT, ";
			sql = sql + DURATION + mapPurpose.get(purpose) + " INT, ";
		}
		sql = sql + "return_call_avg DOUBLE PRECISION, PRIMARY KEY (contract, month));";
		return sql;
	}

	public void createPartitionMonth(Connection connection, String dateString) throws SQLException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		int year = date.getYear();
		int month = date.getMonthOfYear();
		String partition = TABLE_CALL_LOG + "_month_y" + year + "_m" + month;
		String partitionRule = TABLE_CALL_LOG + "_month_insert_y" + year + "_m" + month;
		String partitionIndex = partition + "_index";
		String monthIndex = "y" + year + "_m" + month;
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + partition + " (CHECK (month = '" + monthIndex
				+ "')) INHERITS (" + TABLE_CALL_LOG + ");";
		System.out.println(sqlCreate);
		PostgreSQL.executeUpdateSQL(connection, sqlCreate);
		String sqlRule = "CREATE OR REPLACE RULE " + partitionRule + " AS ON INSERT TO " + TABLE_CALL_LOG
				+ " WHERE (month = '" + monthIndex + "') DO INSTEAD INSERT INTO " + partition + " VALUES (NEW.*);";
		System.out.println(sqlRule);
		PostgreSQL.executeUpdateSQL(connection, sqlRule);
		String sqlIndex = "CREATE INDEX IF NOT EXISTS " + partitionIndex + " ON " + partition + " (contract, month);";
		System.out.println(sqlIndex);
		PostgreSQL.executeUpdateSQL(connection, sqlIndex);
	}
}
