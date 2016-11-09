package com.fpt.ftel.paytv.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.postgresql.PostgreSQL;

public class TableInfoDAO {

	public static void main(String[] args) {
		DateTime d1 = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-10-02");
		DateTime d2 = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-10-02");
		System.out.println(d1.isAfter(d2));

	}

	private static final String SQL_CREATE_TABLE_INFO = "CREATE TABLE IF NOT EXISTS info "
			+ "(contract VARCHAR(22), customer_id VARCHAR(22), "
			+ "location_id INT, location VARCHAR(22), region VARCHAR(22), "
			+ "status_id INT, start_date DATE, stop_date DATE, " + "last_active DATE, "
			+ "pop VARCHAR(10), tap_diem VARCHAR(30), "
			+ "isc_box_type INT, isc_promotion_id INT, isc_start_date DATE, isc_active_date DATE, "
			+ "isc_package TEXT, isc_status_id INT, isc_status_date DATE, " + "PRIMARY KEY(customer_id, contract));";

	public void createTable(Connection connection) throws SQLException {
		System.out.println(SQL_CREATE_TABLE_INFO);
		PostgreSQL.executeUpdateSQL(connection, SQL_CREATE_TABLE_INFO);
	}

	public void updateLastActive(Connection connection, Map<String, String> mapLastActive) throws SQLException {
		String sql = "UPDATE info AS t SET last_active = c.last_active::date FROM (VALUES ";
		for (String id : mapLastActive.keySet()) {
			sql = sql + "('" + id + "','" + mapLastActive.get(id) + "'),";
		}
		sql = sql.substring(0, sql.length() - 1)
				+ ") AS c (customer_id, last_active) WHERE c.customer_id = t.customer_id;";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public Map<String, String> getLastActive(Connection connection, String dateString) throws SQLException {
		Map<String, String> newActive = queryNewLastActive(connection, dateString);
		Map<String, String> oldActive = queryOldLastActive(connection, dateString, newActive.keySet());
		for (String id : newActive.keySet()) {
			if (oldActive.containsKey(id)) {
				DateTime oldDay = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(oldActive.get(id));
				DateTime newDay = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(newActive.get(id));
				if (oldDay.isAfter(newDay)) {
					newActive.put(id, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(oldDay));
				}
			}
		}
		return newActive;
	}

	private Map<String, String> queryNewLastActive(Connection connection, String dateString) throws SQLException {
		PostgreSQL.setConstraintExclusion(connection, true);
		Map<String, String> result = new HashMap<>();
		Statement statement = connection.createStatement();
		String sqlSelect = "SELECT customer_id FROM now WHERE date = '" + dateString + "';";
		ResultSet resultSet = statement.executeQuery(sqlSelect);
		while (resultSet.next()) {
			result.put(resultSet.getString("customer_id"), dateString);
		}
		resultSet.close();
		statement.close();
		return result;
	}

	private Map<String, String> queryOldLastActive(Connection connection, String dateString, Set<String> setUser)
			throws SQLException {
		Map<String, String> result = new HashMap<>();
		Statement statement = connection.createStatement();
		String sqlSelect = "SELECT customer_id,last_active FROM info WHERE customer_id IN ('"
				+ StringUtils.join(setUser, "','") + "');";
		ResultSet resultSet = statement.executeQuery(sqlSelect);
		while (resultSet.next()) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			if (resultSet.getDate("last_active") != null) {
				result.put(resultSet.getString("customer_id"), sdf.format(resultSet.getDate("last_active")));
			}
		}
		resultSet.close();
		statement.close();
		return result;
	}

	public void updateChurnStatus(Connection connection, Map<String, Map<String, String>> mapUserChurnInfo)
			throws SQLException {
		String sql = "UPDATE info AS t SET status_id = c.status_id, stop_date = c.stop_date::date FROM (VALUES ";
		for (String customer_id : mapUserChurnInfo.keySet()) {
			Map<String, String> info = mapUserChurnInfo.get(customer_id);
			sql = sql + "('" + customer_id + "'," + info.get(UserStatus.STATUS_ID) + ",'"
					+ info.get(UserStatus.STOP_DATE) + "'),";
		}
		sql = sql.substring(0, sql.length() - 1)
				+ ") AS c(customer_id, status_id, stop_date) WHERE c.customer_id = t.customer_id;";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void insertNewUser(Connection connection, Map<String, Map<String, String>> mapUserRegisterInfo)
			throws SQLException {
		String sql = "INSERT INTO info (contract, customer_id, location_id, location, region, status_id, start_date) VALUES ";
		for (String customer_id : mapUserRegisterInfo.keySet()) {
			Map<String, String> info = mapUserRegisterInfo.get(customer_id);
			sql = sql + "('" + info.get(UserStatus.CONTRACT).toUpperCase() + "','" + customer_id + "',"
					+ info.get(UserStatus.LOCATION_ID) + ",'" + info.get(UserStatus.LOCATION) + "','"
					+ info.get(UserStatus.REGION) + "'," + info.get(UserStatus.STATUS_ID) + ",'"
					+ info.get(UserStatus.START_DATE) + "'),";
		}
		sql = sql.substring(0, sql.length() - 1) + ";";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updatePop(Connection connection, Map<String, Map<String, String>> mapPop) throws SQLException {
		String sql = "UPDATE info AS t SET pop = c.pop, tap_diem = c.tap_diem FROM (VALUES ";
		for (String contract : mapPop.keySet()) {
			Map<String, String> info = mapPop.get(contract);
			sql = sql + "('" + contract + "','" + info.get(UserStatus.POP) + "','" + info.get(UserStatus.TAP_DIEM)
					+ "'),";
		}
		sql = sql.substring(0, sql.length() - 1) + ") AS c(contract, pop, tap_diem) WHERE c.contract = t.contract;";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public void updateIsc(Connection connection, Map<String, InfoWrapper> mapIsc) throws SQLException {
		String sql = "UPDATE info AS t SET isc_box_type = c.isc_box_type, isc_promotion_id = c.isc_promotion_id, "
				+ "isc_start_date = c.isc_start_date::DATE, isc_active_date = c.isc_active_date::DATE, "
				+ "isc_package = c.isc_package, isc_status_id = c.isc_status_id, isc_status_date = c.isc_status_date::DATE FROM (VALUES ";

		for (String cutomerId : mapIsc.keySet()) {
			InfoWrapper info = mapIsc.get(cutomerId);
			sql = sql + "('" + cutomerId + "','" + info.getIsc_box_type() + "','" + info.getIsc_promotion_id() + "','"
					+ info.getIsc_start_date() + "','" + info.getIsc_active_date() + "','" + info.getIsc_package()
					+ "','" + info.getIsc_status_id() + "','" + info.getIsc_status_date() + "'),";
		}
		sql = sql.substring(0, sql.length() - 1)
				+ ") AS c(customer_id, isc_box_type, isc_promotion_id, isc_start_date, isc_active_date, "
				+ "isc_package, isc_status_id, isc_status_date) WHERE c.customer_id = t.customer_id;";
		PostgreSQL.executeUpdateSQL(connection, sql);
	}

	public static class InfoWrapper {
		Integer isc_box_type;
		Integer isc_promotion_id;
		DateTime isc_start_date;
		DateTime isc_active_date;
		String isc_package;
		Integer isc_status_id;
		DateTime isc_status_date;

		public Integer getIsc_box_type() {
			return isc_box_type;
		}

		public void setIsc_box_type(Integer isc_box_type) {
			this.isc_box_type = isc_box_type;
		}

		public Integer getIsc_promotion_id() {
			return isc_promotion_id;
		}

		public void setIsc_promotion_id(Integer isc_promotion_id) {
			this.isc_promotion_id = isc_promotion_id;
		}

		public DateTime getIsc_start_date() {
			return isc_start_date;
		}

		public void setIsc_start_date(DateTime isc_start_date) {
			this.isc_start_date = isc_start_date;
		}

		public DateTime getIsc_active_date() {
			return isc_active_date;
		}

		public void setIsc_active_date(DateTime isc_active_date) {
			this.isc_active_date = isc_active_date;
		}

		public String getIsc_package() {
			return isc_package;
		}

		public void setIsc_package(String isc_package) {
			this.isc_package = isc_package;
		}

		public Integer getIsc_status_id() {
			return isc_status_id;
		}

		public void setIsc_status_id(Integer isc_status_id) {
			this.isc_status_id = isc_status_id;
		}

		public DateTime getIsc_status_date() {
			return isc_status_date;
		}

		public void setIsc_status_date(DateTime isc_status_date) {
			this.isc_status_date = isc_status_date;
		}

	}
}
