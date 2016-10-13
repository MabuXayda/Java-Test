package com.fpt.ftel.paytv.service;

import java.sql.Connection;
import java.sql.SQLException;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.db.TableDailyDAO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class ProcessTableDaily {
	TableDailyDAO tableDailyDAO;

	public ProcessTableDaily() {
		tableDailyDAO = new TableDailyDAO();
	}

	public void createTable(Connection connection) throws SQLException {
		tableDailyDAO.createTable(connection);
	}

	public void updateTable(Connection connection, DateTime dateTime) throws SQLException {
		long start = System.currentTimeMillis();
		String currentDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		String dropDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime
				.minusDays(Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_DAILY_TIMETOLIVE))));
		tableDailyDAO.dropPartition(connection, dropDateSimple);
		tableDailyDAO.createPartition(connection, currentDateSimple);
		tableDailyDAO.insertFromTable(connection, currentDateSimple);
		PayTVUtils.LOG_INFO.info("------> Done updateDB DAILY | time: " + (System.currentTimeMillis() - start)
				+ " | at: " + System.currentTimeMillis());
	}
}
