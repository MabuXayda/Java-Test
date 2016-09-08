package com.fpt.ftel.paytv.service;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

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

	public void updateTable(Connection connection)
			throws JsonIOException, JsonSyntaxException, FileNotFoundException, SQLException {
		long start = System.currentTimeMillis();
		Set<String> setUserCancel = UserStatus.getSetUserCancel(CommonConfig.get(PayTVConfig.USER_CANCEL_FILE));
		tableChurnDAO.insertChurnDaily(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileSum(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileWeek(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileMonth(connection, setUserCancel);
		PayTVUtils.LOG_INFO.info("Done update CHURN with Time: " + (System.currentTimeMillis() - start) + " | At: "
				+ System.currentTimeMillis());
	}
}
