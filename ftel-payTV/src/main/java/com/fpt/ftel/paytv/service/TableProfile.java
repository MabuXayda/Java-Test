package com.fpt.ftel.paytv.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.db.TableProfileDAO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.fpt.ftel.postgresql.ConnectionFactory;

public class TableProfile {
	TableProfileDAO tableProfileDAO;

	public static void main(String[] args) {
		TableProfile tableProfileService = new TableProfile();
		if (args[0].equals("create table") && args.length == 1) {
			System.out.println("Start create table daily ..........");
			try {
				tableProfileService.processTableProfileCreateTable();
			} catch (SQLException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start table now real job ..........");
			try {
				tableProfileService.processTableProfileReal(args[1]);
			} catch (SQLException | IOException e) {
				PayTVUtils.LOG_ERROR.error(e.getMessage());
				e.printStackTrace();
			}
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public TableProfile() {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_TableProfileService.properties");
		tableProfileDAO = new TableProfileDAO();
	}

	public void processTableProfileCreateTable() throws SQLException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		tableProfileDAO.createTable(connection);
		ConnectionFactory.closeConnection(connection);
	}

	public void processTableProfileReal(String dateString) throws SQLException, IOException {
		Connection connection = ConnectionFactory.openConnection(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_HOST),
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_PORT)),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_DATABASE),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER),
				CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_USER_PASSWORD));
		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.TABLE_PROFILE_SERVICE_MISSING);
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
						.getListDateProcessMissing(ServiceUtils.TABLE_DAILY_SERVICE_MISSING);
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
			System.out.println("Will process: " + willProcess);
			if (willProcess) {
				Map<String, String> mapUserContract = tableProfileDAO.queryUserContract(connection,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(currentDateTime));
				Map<String, Map<String, Integer>> mapUserUsageDaily = tableProfileDAO.queryUserUsageDaily(connection,
						PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(currentDateTime));
				updateProfile(connection, mapUserContract, mapUserUsageDaily, currentDateTime);
				updateProfileML(connection, mapUserUsageDaily, currentDateTime);
			} else {
				listMissing.add(date);
			}

		}
		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.TABLE_PROFILE_SERVICE_MISSING);
		ConnectionFactory.closeConnection(connection);
	}

	private void updateProfile(Connection connection, Map<String, String> mapUserContract,
			Map<String, Map<String, Integer>> mapUserUsageDailyOld, DateTime date) throws SQLException {
		Map<String, Map<String, Integer>> mapUserUsageDailyNew = convertSumToDaily(mapUserUsageDailyOld, date);
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserContract.keySet(), 500);
		updateUserUsageSum(connection, mapUserUsageDailyNew, mapUserContract, listSetUser);
		updateUserUsageWeek(connection, mapUserUsageDailyNew, mapUserContract, date, listSetUser);
		updateUserUsageMonth(connection, mapUserUsageDailyNew, mapUserContract, date, listSetUser);
		mapUserUsageDailyNew = null;
		listSetUser = null;
	}

	private void updateUserUsageWeek(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDaily,
			Map<String, String> mapUserContract, DateTime date, List<Set<String>> listSetUser) throws SQLException {
		String currentDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(date);
		String dropDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(date.minusDays(
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_PROFILE_WEEK_TIMETOLIVE))));
		tableProfileDAO.dropPartitionWeek(connection, dropDateSimple);
		tableProfileDAO.createPartitionWeek(connection, currentDateSimple);

		long start = System.currentTimeMillis();
		int countUpdate = 0;
		int countInsert = 0;
		for (Set<String> setUser : listSetUser) {
			Map<String, Map<String, Integer>> mapUserUsageWeekUpdate = tableProfileDAO.queryUserUsageWeek(connection,
					setUser, currentDateSimple);
			if (mapUserUsageWeekUpdate.size() > 0) {
				for (String customerId : mapUserUsageWeekUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerHard(
							mapUserUsageWeekUpdate.get(customerId), mapUserUsageDaily.get(customerId));
					mapUserUsageWeekUpdate.put(customerId, mapInfo);
				}
				tableProfileDAO.updateUserUsageMultiple(connection, mapUserUsageWeekUpdate, mapUserContract,
						currentDateSimple, "week");
				countUpdate += mapUserUsageWeekUpdate.size();
			}
			Map<String, Map<String, Integer>> mapUserUsageWeekInsert = new HashMap<>();
			for (String customerId : setUser) {
				if (!mapUserUsageWeekUpdate.containsKey(customerId)) {
					mapUserUsageWeekInsert.put(customerId, mapUserUsageDaily.get(customerId));
				}
			}
			if (mapUserUsageWeekInsert.size() > 0) {
				tableProfileDAO.insertUserUsageMultiple(connection, mapUserUsageWeekInsert, mapUserContract,
						currentDateSimple, "week");
				countInsert += mapUserUsageWeekInsert.size();
			}
		}

		int time = (int) (System.currentTimeMillis() - start) / 1000;
		System.out.println("update week: " + countUpdate + " | insert week: " + countInsert + " | time: " + time);
		PayTVUtils.LOG_INFO.info("update week: " + countUpdate + " | insert week: " + countInsert + " | time: " + time);

	}

	private void updateUserUsageMonth(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDaily,
			Map<String, String> mapUserContract, DateTime date, List<Set<String>> listSetUser) throws SQLException {
		String currentDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(date);
		String dropDateSimple = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(date.minusDays(
				Integer.parseInt(CommonConfig.get(PayTVConfig.POSTGRESQL_PAYTV_TABLE_PROFILE_MONTH_TIMETOLIVE))));
		tableProfileDAO.dropPartitionMonth(connection, dropDateSimple);
		tableProfileDAO.createPartitionMonth(connection, currentDateSimple);

		long start = System.currentTimeMillis();
		int countUpdate = 0;
		int countInsert = 0;
		for (Set<String> setUser : listSetUser) {
			Map<String, Map<String, Integer>> mapUserUsageMonthUpdate = tableProfileDAO.queryUserUsageMonth(connection,
					setUser, currentDateSimple);
			if (mapUserUsageMonthUpdate.size() > 0) {
				for (String customerId : mapUserUsageMonthUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerHard(
							mapUserUsageMonthUpdate.get(customerId), mapUserUsageDaily.get(customerId));
					mapUserUsageMonthUpdate.put(customerId, mapInfo);
				}
				tableProfileDAO.updateUserUsageMultiple(connection, mapUserUsageMonthUpdate, mapUserContract,
						currentDateSimple, "month");
				countUpdate += mapUserUsageMonthUpdate.size();
			}
			Map<String, Map<String, Integer>> mapUserUsageMonthInsert = new HashMap<>();
			for (String customerId : setUser) {
				if (!mapUserUsageMonthUpdate.containsKey(customerId)) {
					mapUserUsageMonthInsert.put(customerId, mapUserUsageDaily.get(customerId));
				}
			}
			if (mapUserUsageMonthInsert.size() > 0) {
				tableProfileDAO.insertUserUsageMultiple(connection, mapUserUsageMonthInsert, mapUserContract,
						currentDateSimple, "month");
				countInsert += mapUserUsageMonthInsert.size();
			}
		}

		int time = (int) (System.currentTimeMillis() - start) / 1000;
		System.out.println("update month: " + countUpdate + " | insert month: " + countInsert + " | time: " + time);
		PayTVUtils.LOG_INFO
				.info("update month: " + countUpdate + " | insert month: " + countInsert + " | time: " + time);

	}

	private void updateUserUsageSum(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDaily,
			Map<String, String> mapUserContract, List<Set<String>> listSetUser) throws SQLException {
		long start = System.currentTimeMillis();
		int countUpdate = 0;
		int countInsert = 0;

		for (Set<String> setUser : listSetUser) {
			Map<String, Map<String, Integer>> mapUserUsageSumUpdate = tableProfileDAO.queryUserUsageSum(connection,
					setUser);
			if (mapUserUsageSumUpdate.size() > 0) {
				for (String customerId : mapUserUsageSumUpdate.keySet()) {
					Map<String, Integer> mapInfo = MapUtils.plusMapStringIntegerHard(
							mapUserUsageSumUpdate.get(customerId), mapUserUsageDaily.get(customerId));
					mapUserUsageSumUpdate.put(customerId, mapInfo);
				}
				tableProfileDAO.updateUserUsageMultiple(connection, mapUserUsageSumUpdate, mapUserContract);
				countUpdate += mapUserUsageSumUpdate.size();
			}
			Map<String, Map<String, Integer>> mapUserUsageSumInsert = new HashMap<>();
			for (String customerId : setUser) {
				if (!mapUserUsageSumUpdate.containsKey(customerId)) {
					mapUserUsageSumInsert.put(customerId, mapUserUsageDaily.get(customerId));
				}
			}
			if (mapUserUsageSumInsert.size() > 0) {
				tableProfileDAO.insertUserUsageMultiple(connection, mapUserUsageSumInsert, mapUserContract);
				countInsert += mapUserUsageSumInsert.size();
			}
		}

		int time = (int) (System.currentTimeMillis() - start) / 1000;
		System.out.println("update:" + countUpdate + " | insert: " + countInsert + " | time: " + time);
		PayTVUtils.LOG_INFO.info("update:" + countUpdate + " | insert: " + countInsert + " | time: " + time);
	}

	private void updateProfileML(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDailyOld,
			DateTime date) throws SQLException {
		long startM = System.currentTimeMillis();
		Map<String, Map<String, Integer>> mapUserUsageDailyNew = convertSumToDaily(mapUserUsageDailyOld, date);
		DateTime currentDate = new DateTime().minusDays(1);
		boolean willProcess7 = true;
		boolean willProcess28 = true;
		if (new Duration(date, currentDate).getStandardDays() > 28) {
			willProcess28 = false;
		} else if (new Duration(date, currentDate).getStandardDays() > 7) {
			willProcess7 = false;
		}

		List<Map<String, Map<String, String>>> listMapUserUsageML = splitMap(
				tableProfileDAO.queryUserUsageML(connection), 500);

		for (Map<String, Map<String, String>> mapUserUsageML : listMapUserUsageML) {
			long start = System.currentTimeMillis();
			Map<String, Map<String, Integer>> mapUserUsageML7 = getUserUsageML(7, mapUserUsageML);
			Map<String, Map<String, Integer>> mapUserUsageML28 = getUserUsageML(28, mapUserUsageML);
			Map<String, Map<String, Integer>> mapUserVectorDays = getVector28Days(mapUserUsageML);
			if (willProcess7) {
				Map<String, Map<String, Integer>> mapDailyMinus7 = convertSumToDaily(
						tableProfileDAO.queryUserUsageDaily(connection,
								PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(date.minusDays(7)), mapUserUsageML.keySet()),
						date.minusDays(7));
				mapUserUsageML7 = doPlus(mapUserUsageML7, mapUserUsageDailyNew);
				mapUserUsageML7 = doMinus(mapUserUsageML7, mapDailyMinus7);
			}
			if (willProcess28) {
				Map<String, Map<String, Integer>> mapDailyMinus28 = convertSumToDaily(
						tableProfileDAO.queryUserUsageDaily(connection,
								PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(date.minusDays(28)), mapUserUsageML.keySet()),
						date.minusDays(28));
				mapUserUsageML28 = doPlus(mapUserUsageML28, mapUserUsageDailyNew);
				mapUserUsageML28 = doMinus(mapUserUsageML28, mapDailyMinus28);
				for (String cutomerId : mapUserVectorDays.keySet()) {
					Map<String, Integer> vectorDaysOld = mapUserVectorDays.get(cutomerId);
					Map<String, Integer> vectorDaysNew = getNewVetorDays(currentDate, date, vectorDaysOld,
							mapUserUsageDailyOld.get(cutomerId).get("sum"));
					mapUserVectorDays.put(cutomerId, vectorDaysNew);
				}
			}

			for (String customerId : mapUserUsageML.keySet()) {
				Map<String, String> mapMLUpdate = new HashMap<>();
				mapMLUpdate.put("hourly_ml7", PayTVDBUtils.getJsonVectorHourly(mapUserUsageML7.get(customerId)));
				mapMLUpdate.put("app_ml7", PayTVDBUtils.getJsonVectorApp(mapUserUsageML7.get(customerId)));
				mapMLUpdate.put("daily_ml7", PayTVDBUtils.getJsonVectorDaily(mapUserUsageML7.get(customerId)));
				mapMLUpdate.put("hourly_ml28", PayTVDBUtils.getJsonVectorHourly(mapUserUsageML28.get(customerId)));
				mapMLUpdate.put("app_ml28", PayTVDBUtils.getJsonVectorApp(mapUserUsageML28.get(customerId)));
				mapMLUpdate.put("daily_ml28", PayTVDBUtils.getJsonVectorDaily(mapUserUsageML28.get(customerId)));
				mapMLUpdate.put("days_ml28", PayTVDBUtils.getJsonVectorDays(mapUserVectorDays.get(customerId)));
				mapUserUsageML.put(customerId, mapMLUpdate);
			}
			tableProfileDAO.updateUserUsageMultipleML(connection, mapUserUsageML);
			System.out.println("Done sub updateML with time: " + (System.currentTimeMillis() - start));
		}

		PayTVUtils.LOG_INFO.info("Done big updateML with time: " + (System.currentTimeMillis() - startM));
	}

	private List<Map<String, Map<String, String>>> splitMap(Map<String, Map<String, String>> bigMap, int splitSize) {
		List<Map<String, Map<String, String>>> result = new ArrayList<>();
		Map<String, Map<String, String>> subMap = new HashMap<>();
		int count = 0;
		int total = 0;
		for (String key : bigMap.keySet()) {
			subMap.put(key, bigMap.get(key));
			count++;
			total++;
			if (count == splitSize || total == bigMap.size()) {
				result.add(subMap);
				count = 0;
				subMap = new HashMap<>();
			}
		}
		return result;
	}

	private Map<String, Integer> getNewVetorDays(DateTime currentDate, DateTime date, Map<String, Integer> mapDaysOld,
			Integer valueSum) {
		int duration = (int) new Duration(date, currentDate).getStandardDays();
		String addKey = PayTVDBUtils.VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(duration);
		Map<String, Integer> result = new HashMap<>();
		for (int i = 0; i < 28; i++) {
			String newKey = PayTVDBUtils.VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(i);
			String oldKey = PayTVDBUtils.VECTOR_DAYS_PREFIX + NumberUtils.get2CharNumber(i - 1);
			if (duration == 0) {
				result.put(newKey, mapDaysOld.get(oldKey));
			} else if (duration > 0) {
				result.put(newKey, mapDaysOld.get(newKey));
			}
		}
		result.put(addKey, valueSum);
		return result;
	}

	private Map<String, Map<String, Integer>> doPlus(Map<String, Map<String, Integer>> mapMain,
			Map<String, Map<String, Integer>> mapPlus) {
		for (String id : mapMain.keySet()) {
			Map<String, Integer> main = mapMain.get(id);
			Map<String, Integer> plus = mapPlus.get(id);
			if (plus != null) {
				for (String key : main.keySet()) {
					main.put(key, main.get(key) + (plus.get(key) == null ? 0 : plus.get(key)));
				}
			}
			mapMain.put(id, main);
		}
		return mapMain;
	}

	private Map<String, Map<String, Integer>> doMinus(Map<String, Map<String, Integer>> mapMain,
			Map<String, Map<String, Integer>> mapMinus) {
		for (String id : mapMain.keySet()) {
			Map<String, Integer> main = mapMain.get(id);
			Map<String, Integer> minus = mapMinus.get(id);
			if (minus != null) {
				for (String key : main.keySet()) {
					main.put(key, main.get(key) - (minus.get(key) == null ? 0 : minus.get(key)));
				}
			}
			mapMain.put(id, main);
		}
		return mapMain;
	}

	private Map<String, Map<String, Integer>> getVector28Days(Map<String, Map<String, String>> mapUserUsageML) {
		Map<String, Map<String, Integer>> result = new HashMap<>();
		for (String customerId : mapUserUsageML.keySet()) {
			result.put(customerId, PayTVDBUtils.getVectorDaysFromJson(mapUserUsageML.get(customerId).get("days_ml28")));
		}
		return result;
	}

	private Map<String, Map<String, Integer>> getUserUsageML(int day, Map<String, Map<String, String>> mapUserUsageML) {
		Map<String, Map<String, Integer>> result = new HashMap<>();
		for (String customerId : mapUserUsageML.keySet()) {
			String subFix = null;
			if (day == 7) {
				subFix = PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX;
			} else if (day == 28) {
				subFix = PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
			}
			String hourlyML = mapUserUsageML.get(customerId).get("hourly" + subFix);
			String appML = mapUserUsageML.get(customerId).get("app" + subFix);
			String dailyML = mapUserUsageML.get(customerId).get("daily" + subFix);
			Map<String, Integer> mapUsage = new HashMap<>();
			mapUsage.putAll(PayTVDBUtils.getVectorHourlyFromJson(hourlyML));
			mapUsage.putAll(PayTVDBUtils.getVectorAppFromJson(appML));
			mapUsage.putAll(PayTVDBUtils.getVectorDailyFromJson(dailyML));
			result.put(customerId, mapUsage);
		}
		return result;
	}

	private Map<String, Map<String, Integer>> convertSumToDaily(Map<String, Map<String, Integer>> mapUserUsageDaily,
			DateTime date) {
		Map<String, Map<String, Integer>> result = new HashMap<>();
		String dayOfWeek = PayTVDBUtils.VECTOR_DAILY_PREFIX + DateTimeUtils.getDayOfWeek(date).toLowerCase();
		for (String customerId : mapUserUsageDaily.keySet()) {
			Map<String, Integer> mapUsageNew = new HashMap<>();
			mapUsageNew.putAll(mapUserUsageDaily.get(customerId));
			for (String day : DateTimeUtils.LIST_DAY_OF_WEEK) {
				mapUsageNew.put(PayTVDBUtils.VECTOR_DAILY_PREFIX + day.toLowerCase(), 0);
			}
			mapUsageNew.put(dayOfWeek, mapUsageNew.get("sum"));
			mapUsageNew.remove("sum");
			result.put(customerId, mapUsageNew);
		}
		return result;
	}
}
