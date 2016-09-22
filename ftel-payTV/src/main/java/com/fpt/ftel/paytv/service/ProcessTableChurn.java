package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.db.TableChurnDAO;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class ProcessTableChurn {
	TableChurnDAO tableChurnDAO;

	public ProcessTableChurn() {
		tableChurnDAO = new TableChurnDAO();
	}

	public void createTable(Connection connection) throws SQLException {
		tableChurnDAO.createTable(connection);
	}

	public void updateTable(Connection connection, DateTime dateTime)
			throws JsonIOException, JsonSyntaxException, SQLException, IOException {
		long start = System.currentTimeMillis();
		String dateString = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime);
		URL url = new URL(CommonConfig.get(PayTVConfig.GET_USER_CHURN_API) + dateString);
		String content = IOUtils.toString(url, "UTF-8");
		Set<String> setUserCancel = UserStatus.getSetUserCancelFromString(content);
		PayTVUtils.LOG_INFO.info("User churn " + dateString + ": " + setUserCancel.size());
		
		tableChurnDAO.insertChurnDaily(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileSum(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileWeek(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileMonth(connection, setUserCancel);
		PayTVUtils.LOG_INFO.info("Done update CHURN with Time: " + (System.currentTimeMillis() - start) + " | At: "
				+ System.currentTimeMillis());
	}
}
