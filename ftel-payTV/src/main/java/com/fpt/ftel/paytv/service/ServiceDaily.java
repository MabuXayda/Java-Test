package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class ServiceDaily {
	ProcessTableDaily processTableDaily;
	ProcessTableProfile processTableProfile;
	ProcessTableChurn processTableChurn;

	public static void main(String[] args) {
		ServiceDaily serviceDaily = new ServiceDaily();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table ..........");
			try {
				serviceDaily.processCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start real job ..........");
			try {
				serviceDaily.processTableReal(args[1]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("fix") && args.length == 3) {
			System.out.println("Start table now fix job ..........");
			try {
				serviceDaily.processTableFix(args[1], args[2]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else {
			System.out.println("- Fix command: java -jar .jar fix yyyy-mm-dd_HH yyyy-mm-dd_HH");
			System.out.println("note: fix [from day dd date1 to day dd date2] ");
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceDaily() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_ServiceDaily.properties");
		processTableDaily = new ProcessTableDaily();
		processTableProfile = new ProcessTableProfile();
		processTableChurn = new ProcessTableChurn();
	}

	public void processTableReal(String dateString) throws IOException, SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.DAILY_SERVICE_MISSING);
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
			if (willProcess) {
				processTableDaily.updateTable(connection, currentDateTime);
				processTableProfile.updateTable(connection, currentDateTime);
				processTableChurn.updateTable(connection, currentDateTime);
			} else {
				listMissing.add(date);
			}
		}
		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.DAILY_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	public void processTableFix(String fromDate, String toDate) throws IOException, SQLException {
		DateTime beginDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(fromDate);
		DateTime endDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(toDate);
		while (new Duration(beginDate, endDate).getStandardSeconds() >= 0) {
			processTableReal(PayTVUtils.FORMAT_DATE_TIME.print(beginDate));
			beginDate = beginDate.plusDays(1);
		}
	}

	public void processCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		processTableDaily.createTable(connection);
		processTableProfile.createTable(connection);
		processTableChurn.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

}
