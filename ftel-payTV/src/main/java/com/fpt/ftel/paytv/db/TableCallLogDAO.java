package com.fpt.ftel.paytv.db;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
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
	private Map<String, String> mapPurpose;
	private List<String> listPurpose;

	public TableCallLogDAO() {
		mapPurpose = getMapCallLogPurpose();
		listPurpose = new ArrayList<>(mapPurpose.keySet());
	}

	public static Map<String, String> getMapCallLogPurpose() {
		try {
			return new Gson().fromJson(
					new JsonReader(new FileReader(CommonConfig.get(PayTVConfig.CALL_LOG_PURPOSE_FILE))),
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

	public void createTable(Connection connection) throws SQLException {
		String sql = getSQLCreateTableCallLog();
		System.out.println(sql);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	private String getSQLCreateTableCallLog() {
		Map<String, String> mapPurpose = getMapCallLogPurpose();
		String sql = "CREATE TABLE IF NOT EXISTS " + TABLE_CALL_LOG + " (contract VARCHAR(22), month VARCHAR(10), ";
		for (String purpose : listPurpose) {
			sql = sql + COUNT + mapPurpose.get(purpose) + " INT, ";
		}
		for (String purpose : listPurpose) {
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

	public void insertUserUsageMultiple(Connection connection, Map<String, Map<String, Integer>> mapContractCallCount,
			Map<String, Map<String, Integer>> mapContractCallDuration, Map<String, Double> mapContractRecallAvg,
			String dateString) throws SQLException {
		String sql = generatedSQLInsertUserCallLogMultiple(mapContractCallCount, mapContractCallDuration,
				mapContractRecallAvg, dateString);
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	private String generatedSQLInsertUserCallLogMultiple(Map<String, Map<String, Integer>> mapContractCallCount,
			Map<String, Map<String, Integer>> mapContractCallDuration, Map<String, Double> mapContractRecallAvg,
			String dateString) {
		String sql = "INSERT INTO " + TABLE_CALL_LOG + " " + generatedStringColumn() + " VALUES ";
		for (String contract : mapContractCallCount.keySet()) {
			sql = sql
					+ generatedStringValue(contract, mapContractCallCount.get(contract),
							mapContractCallDuration.get(contract), mapContractRecallAvg.get(contract), dateString)
					+ ",";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql = sql + ";";
		return sql;
	}

	private String generatedStringValue(String contract, Map<String, Integer> mapCallCount,
			Map<String, Integer> mapCallDuration, Double recallAvg, String dateString) {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString);
		String monthIndex = "y" + date.getYear() + "_m" + date.getMonthOfYear();
		String value = "('" + contract + "','" + monthIndex + "'";
		for (String purpose : listPurpose) {
			value = value + "," + mapCallCount.get(purpose);
		}
		for (String purpose : listPurpose) {
			value = value + "," + mapCallDuration.get(purpose);
		}
		value = value + "," + recallAvg + ")";
		return value;
	}

	private String generatedStringColumn() {
		String col = "(contract,month";
		for (String purpose : listPurpose) {
			col = col + "," + COUNT + mapPurpose.get(purpose);
		}
		for (String purpose : listPurpose) {
			col = col + "," + DURATION + mapPurpose.get(purpose);
		}
		col = col + ",return_call_avg)";
		return col;
	}
}
