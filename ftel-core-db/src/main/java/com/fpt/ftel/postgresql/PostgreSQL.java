package com.fpt.ftel.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQL {

	public static void executeSQL(Connection connection, String sql) throws SQLException {
		Statement statement = connection.createStatement();
		statement.execute(sql);
		statement.close();
		connection.commit();
	}

	public static void executeUpdateSQL(Connection connection, String sql) throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();
		connection.commit();
	}

	public static void setConstraintExclusion(Connection connection, boolean b) throws SQLException {
		String sql;
		if (b) {
			sql = "SET constraint_exclusion = on";
		} else {
			sql = "SET constraint_exclusion = off";
		}
		executeSQL(connection, sql);
	}

}
