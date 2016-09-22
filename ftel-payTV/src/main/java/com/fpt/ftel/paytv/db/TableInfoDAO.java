package com.fpt.ftel.paytv.db;

public class TableInfoDAO {
	private static final String SQL_CREATE_TABLE_NOW = "CREATE TABLE IF NOT EXISTS info (contract VARCHAR(22), customer_id VARCHAR(22), "
			+ "service_id INT, service_name TEXT, location TEXT, status INT, start_date DATE, stop_date DATE, last_active DATE "
			+ "PRIMARY KEY(customer_id);";
}
