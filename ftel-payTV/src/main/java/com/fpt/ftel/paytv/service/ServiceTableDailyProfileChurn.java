package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.db.TableDailyDAO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceTableDailyProfileChurn {
	TableDailyDAO tableDailyDAO;

	public static void main(String[] args) throws IOException {
		ServiceTableDailyProfileChurn tableDailyService = new ServiceTableDailyProfileChurn();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table daily ..........");
			try {
				tableDailyService.processTableDailyCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start table now real job ..........");
			try {
				tableDailyService.processTableDailyReal(args[1]);
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceTableDailyProfileChurn() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableDailyService.properties");
		tableDailyDAO = new TableDailyDAO();
	}

	public void processTableDailyReal(String dateString) throws IOException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.TABLE_DAILY_SERVICE_MISSING);
		List<String> listMissing = new ArrayList<>();
		DateTime currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(currentDateTime.minusDays(1)));

		for (String date : listDateString) {
			currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);
			

			boolean willProcess = false;
			int wait = 0;
			while (willProcess == false && wait < 4) {
				willProcess = true;
				List<DateTime> listDateTimePreServiceUnprocessed = ServiceUtils
						.getListDateProcessMissing(ServiceUtils.TABLE_NOW_SERVICE_MISSING);
				for (DateTime dateTimeUnprocessed : listDateTimePreServiceUnprocessed) {
					if (DateTimeComparator.getDateOnlyInstance().compare(dateTimeUnprocessed, currentDateTime) == 0) {
						willProcess = false;
						break;
					}
				}
				if (willProcess == false) {
					wait++;
					try {
						Thread.sleep(30 * 60 * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			System.out.println("Will process: " + willProcess);
			if (willProcess) {
				processDaily(connection, currentDateTime);
			} else {
				listMissing.add(date);
			}

		}
		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.TABLE_DAILY_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	public void processTableDailyCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableDailyDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}
	
	private void processDaily(Connection connection, DateTime dateTime) throws SQLException{
		String currentDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		String dropDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime.minusDays(
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_DAILY_TIMETOLIVE))));
		tableDailyDAO.dropPartition(connection, dropDateSimple);
		tableDailyDAO.createPartition(connection, currentDateSimple);
		tableDailyDAO.insertFromTable(connection, currentDateSimple);
	}
}
