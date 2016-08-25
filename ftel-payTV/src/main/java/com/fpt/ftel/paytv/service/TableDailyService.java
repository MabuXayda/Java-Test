package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.config.PayTVConfig;
import com.fpt.ftel.paytv.db.SQLTableDaily;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class TableDailyService {
	private static final String TABLE = "daily";

	public static void main(String[] args) throws IOException {
		TableDailyService tableDailyService = new TableDailyService();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start table daily create table job ..........");
			tableDailyService.processTableDailyCreateTable();
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start table now real job ..........");
			tableDailyService.processTableDailyReal(args[1]);
		} else {
			System.out.println("Usage : ");
			System.out.println("create table: \"create table\"");
			System.out.println("real: real dateString");
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public TableDailyService() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableDailyService.properties");
	}

	public void processTableDailyReal(String dateString) throws IOException {
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
			String currentSimpleDate = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(currentDateTime);
			String dropSimpleDate = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(currentDateTime.minusDays(
					Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_DAILY_TIMETOLIVE))));
			SQLTableDaily.dropPartition(connection, dropSimpleDate);
			SQLTableDaily.createPartition(connection, currentSimpleDate);
			
			boolean willProcess = false;
			int wait = 0;
			while (willProcess == false && wait < 10) {
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
						Thread.sleep(5 * 60 * 1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			System.out.println("Will process: " + willProcess);
			if (willProcess) {
				SQLTableDaily.insertFromTable(connection, currentSimpleDate);
			} else {
				listMissing.add(date);
			}

		}
		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.TABLE_DAILY_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	public void processTableDailyCreateTable() {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		SQLTableDaily.createTable(connection, TABLE);
		ConnectionFactory.closeConnection(connection);
	}
}
