package com.fpt.ftel.paytv.service;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.db.TableChurnDAO;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.postgresql.ConnectionFactory;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class TableChurn {
	TableChurnDAO tableChurnDAO;
	
	public TableChurn() {
		tableChurnDAO = new TableChurnDAO();
	}
	
	public void processTableChurnCreateTable() throws SQLException{
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableChurnDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}
	
	public void updateTableChurn(Connection connection) throws JsonIOException, JsonSyntaxException, FileNotFoundException, SQLException{
		Set<String> setUserCancel = UserStatus.getSetUserCancel(CommonConfig.get(PayTVConfig.USER_CANCEL_FILE));
		TableChurnDAO tableChurnDAO = new TableChurnDAO();
		tableChurnDAO.insertChurnDaily(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileSum(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileWeek(connection, setUserCancel);
		tableChurnDAO.insertChurnProfileMonth(connection, setUserCancel);
	}
}
