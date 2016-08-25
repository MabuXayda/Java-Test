package com.fpt.ftel.postgresql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQL {

	public static String generatedSQLCreateTriggerDeleteOldRows(String table, String timeStampCol, int days) {
		String sql = "CREATE FUNCTION delete_old_rows() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN DELETE FROM "
				+ table + " WHERE " + timeStampCol + " < NOW() - INTERVAL '" + days + " days'; RETURN NULL; END; $$;";
		return sql;
	}

	public static String generatedSQLCallingTriggger(String table) {
		String sql = "CREATE TRIGGER trigger_delete_old_rows AFTER INSERT ON " + table
				+ " EXECUTE PROCEDURE delete_old_rows();";
		return sql;
	}

	public static String generatedSQLDeleteOldRows(String table, String timeStampCol, int days) {
		String sql = "DELETE FROM " + table + " WHERE " + timeStampCol + " < NOW() - INTERVAL '" + days + " days';";
		return sql;
	}

	public static void executeSQL(Connection connection, String sql) throws SQLException {
		connection.setAutoCommit(false);
		Statement statement = connection.createStatement();
		// System.out.println("SQL: " + sql);
		statement.executeUpdate(sql);
		statement.close();
		connection.commit();
	}
}
