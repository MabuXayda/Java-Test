package com.fpt.ftel.paytv.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.paytv.db.TableProfileDAO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVDBUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class ProcessTableProfile {
	TableProfileDAO tableProfileDAO;

	public ProcessTableProfile() {
		tableProfileDAO = new TableProfileDAO();
	}

	public void createTable(Connection connection) throws SQLException {
		tableProfileDAO.createTable(connection);
	}

	public void updateTable(Connection connection, DateTime dateTime) throws SQLException {
		Map<String, String> mapUserContract = tableProfileDAO.queryUserContract(connection,
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
		Map<String, Map<String, Integer>> mapUserUsageDailyOld = tableProfileDAO.queryUserUsageDaily(connection,
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
		Map<String, Map<String, Integer>> mapUserUsageDailyNew = PayTVDBUtils.convertSumToDaily(mapUserUsageDailyOld,
				dateTime);
		updateProfile(connection, mapUserContract, mapUserUsageDailyOld, mapUserUsageDailyNew, dateTime);
		updateProfileML(connection, mapUserUsageDailyOld, mapUserUsageDailyNew, dateTime);
	}

	private void updateProfile(Connection connection, Map<String, String> mapUserContract,
			Map<String, Map<String, Integer>> mapUserUsageDailyOld,
			Map<String, Map<String, Integer>> mapUserUsageDailyNew, DateTime date) throws SQLException {
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(mapUserContract.keySet(), 500);
		updateProfileSum(connection, mapUserUsageDailyNew, mapUserContract, listSetUser);
		updateProfileWeek(connection, mapUserUsageDailyNew, mapUserContract, date, listSetUser);
		updateProfileMonth(connection, mapUserUsageDailyNew, mapUserContract, date, listSetUser);
		listSetUser = null;
	}

	private void updateProfileWeek(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDaily,
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

		System.out.println("Done update profile WEEK: " + countUpdate + " | insert week: " + countInsert + " | time: "
				+ (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
		PayTVUtils.LOG_INFO.info("Done update profile WEEK: " + countUpdate + " | insert week: " + countInsert
				+ " | time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());

	}

	private void updateProfileMonth(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDaily,
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

		System.out.println("Done update profile MONTH: " + countUpdate + " | insert month: " + countInsert + " | time: "
				+ (System.currentTimeMillis() - start));
		PayTVUtils.LOG_INFO.info("Done update profile MONTH: " + countUpdate + " | insert month: " + countInsert
				+ " | time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());

	}

	private void updateProfileSum(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDaily,
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

		System.out.println("Done update profile SUM:" + countUpdate + " | insert: " + countInsert + " | time: "
				+ (System.currentTimeMillis() - start));
		PayTVUtils.LOG_INFO.info("Done update profile SUM:" + countUpdate + " | insert: " + countInsert + " | time: "
				+ (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
	}

	private void updateProfileML(Connection connection, Map<String, Map<String, Integer>> mapUserUsageDailyOld,
			Map<String, Map<String, Integer>> mapUserUsageDailyNew, DateTime dateTime) throws SQLException {
		long startM = System.currentTimeMillis();
		DateTime currentDate = new DateTime().minusDays(1);
		boolean willProcess7 = true;
		boolean willProcess28 = true;
		if (new Duration(dateTime, currentDate).getStandardDays() > 28) {
			willProcess28 = false;
		} else if (new Duration(dateTime, currentDate).getStandardDays() > 7) {
			willProcess7 = false;
		}

		List<Map<String, Map<String, String>>> listMapUserUsageML = splitMap(
				tableProfileDAO.queryUserUsageML(connection), 500);

		for (Map<String, Map<String, String>> mapUserUsageML : listMapUserUsageML) {
			long start = System.currentTimeMillis();
			Map<String, Map<String, Integer>> mapUserUsageML7 = getML7(mapUserUsageML);
			Map<String, Map<String, Integer>> mapUserUsageML28 = getML28(mapUserUsageML);
			Map<String, Map<String, Integer>> mapUserVectorDays = getMLDays(mapUserUsageML);
			if (willProcess7) {
				Map<String, Map<String, Integer>> mapDailyMinus7 = PayTVDBUtils
						.convertSumToDaily(tableProfileDAO.queryUserUsageDaily(connection,
								PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime.minusDays(7)),
								mapUserUsageML.keySet()), dateTime.minusDays(7));
				mapUserUsageML7 = doPlus(mapUserUsageML7, mapUserUsageDailyNew);
				mapUserUsageML7 = doMinus(mapUserUsageML7, mapDailyMinus7);
			}
			if (willProcess28) {
				Map<String, Map<String, Integer>> mapDailyMinus28 = PayTVDBUtils
						.convertSumToDaily(tableProfileDAO.queryUserUsageDaily(connection,
								PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime.minusDays(28)),
								mapUserUsageML.keySet()), dateTime.minusDays(28));
				mapUserUsageML28 = doPlus(mapUserUsageML28, mapUserUsageDailyNew);
				mapUserUsageML28 = doMinus(mapUserUsageML28, mapDailyMinus28);
				for (String cutomerId : mapUserVectorDays.keySet()) {
					Map<String, Integer> vectorDaysOld = mapUserVectorDays.get(cutomerId);
					Map<String, Integer> vectorDaysNew = getNewVectorDays(currentDate, dateTime, vectorDaysOld,
							mapUserUsageDailyOld.get(cutomerId) == null ? 0
									: mapUserUsageDailyOld.get(cutomerId).get("sum"));
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
			System.out.println("Done sub update profile ML with time: " + (System.currentTimeMillis() - start));
		}

		PayTVUtils.LOG_INFO.info("Done big update profile ML with time: " + (System.currentTimeMillis() - startM)
				+ " | At: " + System.currentTimeMillis());
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

	private Map<String, Integer> getNewVectorDays(DateTime currentDate, DateTime date, Map<String, Integer> mapDaysOld,
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

	private Map<String, Map<String, Integer>> getMLDays(Map<String, Map<String, String>> mapUserUsageML) {
		Map<String, Map<String, Integer>> result = new HashMap<>();
		for (String customerId : mapUserUsageML.keySet()) {
			result.put(customerId, PayTVDBUtils.getVectorDaysFromJson(mapUserUsageML.get(customerId).get("days_ml28")));
		}
		return result;
	}

	private Map<String, Map<String, Integer>> getML7(Map<String, Map<String, String>> mapUserUsageML) {
		Map<String, Map<String, Integer>> result = new HashMap<>();
		for (String customerId : mapUserUsageML.keySet()) {
			String subFix = PayTVDBUtils.MACHINE_LEARNING_7_SUBFIX;
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

	private Map<String, Map<String, Integer>> getML28(Map<String, Map<String, String>> mapUserUsageML) {
		Map<String, Map<String, Integer>> result = new HashMap<>();
		for (String customerId : mapUserUsageML.keySet()) {
			String subFix = PayTVDBUtils.MACHINE_LEARNING_28_SUBFIX;
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

}
