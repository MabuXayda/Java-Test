package com.fpt.ftel.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQL {

	public static void executeSQL(Connection connection, String sql) throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();
		connection.commit();
	}

	public static void executeUpdateSQL(Connection connection, String sql) throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();
		connection.commit();
	}

}
