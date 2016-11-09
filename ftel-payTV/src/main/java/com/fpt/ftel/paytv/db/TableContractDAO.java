package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.fpt.ftel.paytv.db.wrapper.TableContractWrapper;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableContractDAO {
	private static String TABLE_CONTRACT = "contract";

	private static final String SQL_CREATE_TABLE_CONTRACT = "CREATE TABLE IF NOT EXISTS " + TABLE_CONTRACT
			+ " (contract VARCHAR(22), box_count INT, payment_method TEXT, "
			+ "point_set VARCHAR(30), location VARCHAR(22), region VARCHAR(22), "
			+ "status_id INT, start_date DATE, stop_date DATE, stop_reason TEXT, " + "late_pay_score DOUBLE PRECISION, "
			+ "PRIMARY KEY(contract));";

	public void createTable(Connection connection) throws SQLException {
		System.out.println(SQL_CREATE_TABLE_CONTRACT);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_CONTRACT);
	}

	public void insertNewContract(Connection connection, Map<String, TableContractWrapper> mapContractWrapper)
			throws SQLException {
		String sql = "INSERT INTO " + TABLE_CONTRACT
				+ " (contract, box_count, payment_method, point_set, location, region, status_id, start_date, stop_date, stop_reason, late_pay_score) VALUES ";
		for (String contract : mapContractWrapper.keySet()) {
			TableContractWrapper wrapper = mapContractWrapper.get(contract);
			sql = sql + "('" + wrapper.getContract().toUpperCase() + "'," + wrapper.getBox_count() + ",'"
					+ wrapper.getPayment_method() + "','" + wrapper.getPoint_set() + "','" + wrapper.getLocation()
					+ "','" + wrapper.getRegion() + "'," + wrapper.getStatus_id() + ",'" + wrapper.getStart_date()
					+ "','" + wrapper.getStop_date() + "','" + wrapper.getStop_reason() + "',"
					+ wrapper.getLate_pay_score() + "),";
		}
		sql = sql.substring(0, sql.length() - 1) + ";";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

}
