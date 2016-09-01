package com.fpt.ftel.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
	private static ConnectionFactory instance = new ConnectionFactory();
	public static final String DRIVER_CLASS = "org.postgresql.Driver";

	private ConnectionFactory() {
		try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Connection createConnection(String host, int port, String database, String user, String password) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database, user,
					password);
			connection.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}

	public static Connection openConnection(String host, int port, String database, String user, String password) {
		return instance.createConnection(host, port, database, user, password);
	}

	public static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
