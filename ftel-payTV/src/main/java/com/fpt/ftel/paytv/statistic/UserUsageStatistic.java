package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class UserUsageStatistic {
	private static Map<String, Map<String, Integer>> mapUserVectorApp;
	private static Map<String, Map<String, Integer>> mapUserVectorDaily;
	private static Map<String, Map<Integer, Integer>> mapUserVectorHourly;
	private static Map<String, Map<String, Integer>> mapUserLogIdCount;
	private static Map<String, Map<Integer, Integer>> mapUserVectorDays;
	private static Set<String> setLogId;

	public static void main(String[] args) throws IOException {
//		UserUsageStatistic userUsage = new UserUsageStatistic();
//		userUsage.processStatistic();
		DateTime beginDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime("2016-09-16_00");
		DateTime endDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime("2016-09-16_00");
		while (new Duration(beginDate, endDate).getStandardSeconds() >= 0) {
			System.out.println(beginDate);
			beginDate = beginDate.plusHours(1);
		}
	}

	private void initMap() {
		mapUserVectorHourly = new ConcurrentHashMap<>();
		mapUserVectorDaily = new ConcurrentHashMap<>();
		mapUserVectorApp = new ConcurrentHashMap<>();
		setLogId = ConcurrentHashMap.newKeySet();
		mapUserLogIdCount = new ConcurrentHashMap<>();
		mapUserVectorDays = new ConcurrentHashMap<>();
	}

	public void processStatistic() throws IOException {
		HdfsIO hdfsIOSimple = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE),
				CommonConfig.get(PayTVConfig.HDFS_SITE));
		long start = System.currentTimeMillis();
		System.out.println("Start t7");
		DateTime dateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime("2016-07-31 00:00:00");

		List<String> listFilePathInput = new ArrayList<>();
		// = getListLogPathHdfsStatistic(dateTime.minusMonths(1), hdfsIOSimple);
		listFilePathInput.addAll(getListLogPathHdfsStatistic(dateTime, hdfsIOSimple));

		String outputFolderPath = CommonConfig.get(PayTVConfig.MAIN_DIR) + "/zCheck_t7";
		FileUtils.createFolder(outputFolderPath);

		Map<String, DateTime> mapUserChurnCondition = new HashMap<>();
		BufferedReader br = new BufferedReader(
				new FileReader(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/z_t7/churn_t7.csv"));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			mapUserChurnCondition.put(line.split(",")[0],
					PayTVUtils.FORMAT_DATE_TIME.parseDateTime(line.split(",")[1]));
			line = br.readLine();
		}
		br.close();
		// PrintWriter pr = new PrintWriter(new FileWriter(outputFolderPath +
		// "/churn_t7.csv"));
		// pr.println("CustomerId,StopDate");
		// for (String id : mapUserChurnCondition.keySet()) {
		// pr.println(id + "," +
		// PayTVUtils.FORMAT_DATE_TIME.print(mapUserChurnCondition.get(id)));
		// }
		// pr.close();
		// System.out.println("<<<<<<< DONE GET LIST USER CHURN T7");
		//
		statisticUserUsageBlind(mapUserChurnCondition, dateTime, listFilePathInput, outputFolderPath, hdfsIOSimple,
				false, false, false, false, true);

		// statisticUserUsageCheck(mapUserChurnCondition, listFilePathInput,
		// hdfsIOSimple);
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DONE T7 | Time: "
				+ (System.currentTimeMillis() - start));

	}

	private Map<String, DateTime> getMapUserDateConditionLocalStatistic() throws IOException {
		Map<String, DateTime> mapChurn_t2 = UserStatus
				.getMapUserChurnDateCondition(CommonConfig.get(PayTVConfig.SUPPORT_DATA_DIR) + "/userChurn_t2.csv");

		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		mapUserDateCondition.putAll(mapChurn_t2);
		return mapUserDateCondition;
	}

	private List<String> getListLogPathHdfsStatistic(DateTime dateTime, HdfsIO hdfsIOSimple) throws IOException {
		int year = dateTime.getYear();
		int month = dateTime.getMonthOfYear();
		String path = CommonConfig.get(PayTVConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/"
				+ NumberUtils.get2CharNumber(month);
		List<String> listLogPath = hdfsIOSimple.getListFileInDir(path);
		FileUtils.sortListFilePathDateTimeHdfs(listLogPath);
		return listLogPath;
	}

	private void printUserUsageStatistic(String outputFolderPath, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processDays) throws IOException {
		if (processHourly) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorHourly.csv"));
			StatisticUtils.printHourly(pr, mapUserVectorHourly);
		}
		if (processDaily) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorDaily.csv"));
			StatisticUtils.printDaily(pr, mapUserVectorDaily);
		}
		if (processApp) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorApp.csv"));
			StatisticUtils.printApp(pr, mapUserVectorApp, PayTVUtils.SET_APP_NAME_RTP);
		}
		if (processLogId) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/logIdCount.csv"));
			StatisticUtils.printLogIdCount(pr, setLogId, mapUserLogIdCount);
		}
		if (processDays) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorDays.csv"));
			StatisticUtils.printDays(pr, mapUserVectorDays);
		}

	}

	private Set<String> getSetNoLog() throws IOException {
		Set<String> result = new HashSet<>();
		BufferedReader br = new BufferedReader(
				new FileReader(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/z_t7/churn_no_log.csv"));
		String line = br.readLine();
		while (line != null) {
			result.add(line.split(",")[0]);
			line = br.readLine();
		}
		br.close();
		return result;
	}

	private void statisticUserUsageCheck(Map<String, DateTime> mapUserChurnDateCondition, List<String> listFileLogPath,
			HdfsIO hdfsIOSimple) throws IOException {
		Set<String> setNoLog = getSetNoLog();
		PrintWriter pr = new PrintWriter(new FileWriter(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/checkNoLog.txt"));
		PrintWriter prErr = new PrintWriter(
				new FileWriter(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/checkErrorLog.txt"));
		// ExecutorService executorService = Executors.newFixedThreadPool(4);
		for (final String filePath : listFileLogPath) {
			// executorService.execute(new Runnable() {
			//
			// @Override
			// public void run() {
			BufferedReader br = hdfsIOSimple.getReadStream(filePath);
			String line = br.readLine();
			int count = 0;
			int total = 0;
			while (line != null) {
				try {
					total++;
					String[] arr = line.split(",");
					String customerId = arr[0];
					String logId = arr[2];
					String appName = arr[3];
					DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);
					int dayDuration = 0;
					if (mapUserChurnDateCondition.containsKey(customerId) && received_at != null
							&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
						dayDuration = DateTimeUtils.getDayDuration(received_at,
								mapUserChurnDateCondition.get(customerId));
						if (dayDuration >= 0 && dayDuration <= 27) {
							if (setNoLog.contains(customerId)) {
								pr.println(line);
								count++;
							}
						}
					}
				} catch (Exception e) {
					prErr.println(line);
				}
				line = br.readLine();
			}
			br.close();
			System.out.println("Done file: " + filePath + " | total:" + total + " | count:" + count);
		}

		// });
		// }
		pr.close();
	}

	private void statisticUserUsageBlind(Map<String, DateTime> mapUserChurnDateCondition, DateTime dateTime,
			List<String> listFileLogPath, String outputFolderPath, HdfsIO hdfsIOSimple, boolean processHourly,
			boolean processDaily, boolean processApp, boolean processLogId, boolean processDays) throws IOException {
		String pathCheckId = outputFolderPath + "/log";
		List<String> listCheckId = Arrays.asList("285563", "285564", "285565");
		PrintWriter prWTF = new PrintWriter(new FileWriter(outputFolderPath + "/checkWTF.csv"));

		DateTime dayCondition = dateTime.plusDays(1);
		System.out.println("Day condition: " + dayCondition);
		initMap();
		// ExecutorService executorService = Executors.newFixedThreadPool(4);
		for (final String filePath : listFileLogPath) {
			// executorService.execute(new Runnable() {
			// @Override
			// public void run() {
			long start = System.currentTimeMillis();
			int countTotal = 0;
			int countTime = 0;
			int countLogId = 0;

			Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
			Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
			Map<String, DateTime> mapCheckValidRTP = new HashMap<>();

			BufferedReader br = null;
			String line = null;
			try {
				br = hdfsIOSimple.getReadStream(filePath);
				line = br.readLine();
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {
					String customerId = arr[0];
					String logId = arr[2];
					String appName = arr[3];
					Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
					String unparseSMM = arr[6];
					DateTime sessionMainMenu = PayTVUtils.parseSessionMainMenu(arr[6]);
					DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);
					int secondsSMM = 0;
					int secondsRTP = 0;
					boolean willProcessCountLogId = false;
					boolean willProcessSMM = false;
					boolean willProcessRTP = false;
					int dayDuration = 0;

					if (StringUtils.isNumeric(customerId) && received_at != null
							&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
						if (!mapCheckDupSMM.containsKey(customerId)) {
							mapCheckDupSMM.put(customerId, new HashSet<>());
						}
						dayDuration = DateTimeUtils.getDayDuration(received_at,
								mapUserChurnDateCondition.get(customerId) == null ? dayCondition
										: mapUserChurnDateCondition.get(customerId));

						if (dayDuration >= 0 && dayDuration <= 27) {

							if (listCheckId.contains(customerId)) {
								PrintWriter prCheckId = new PrintWriter(
										new FileWriter(pathCheckId + "_" + customerId + ".csv", true));
								prCheckId.println(line);
								prCheckId.close();
							}

							willProcessCountLogId = true;
							if (logId.equals("12") || logId.equals("18")) {
								if (sessionMainMenu != null) {
									willProcessSMM = StatisticUtils.willProcessSessionMainMenu(customerId, unparseSMM,
											sessionMainMenu, received_at, mapCheckDupSMM, mapCheckValidSMM);
									if (willProcessSMM) {
										secondsSMM = (int) new Duration(sessionMainMenu, received_at)
												.getStandardSeconds();
										if (secondsSMM <= 0 || secondsSMM > 12 * 3600) {
											willProcessSMM = false;
										}
									}
								}
								willProcessCountLogId = willProcessSMM;
							} else if (PayTVUtils.SET_APP_NAME_RTP.contains(appName)
									&& PayTVUtils.SET_LOG_ID_RTP.contains(logId)) {
								willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId, received_at,
										realTimePlaying, mapCheckValidRTP);
								if (willProcessRTP) {
									secondsRTP = (int) Math.round(realTimePlaying);
									if (secondsRTP <= 0 || secondsRTP > 3 * 3600) {
										willProcessRTP = false;
									}
								}
								willProcessCountLogId = willProcessRTP;
							}
						}
					}
					if (willProcessRTP) {
						countTime++;
						if (processHourly) {
							Map<Integer, Integer> mapHourly = mapUserVectorHourly.get(customerId) == null
									? new ConcurrentHashMap<>() : mapUserVectorHourly.get(customerId);
							StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
							mapUserVectorHourly.put(customerId, mapHourly);
						}
						if (processDaily) {
							Map<String, Integer> mapDaily = mapUserVectorDaily.get(customerId) == null
									? new ConcurrentHashMap<>() : mapUserVectorDaily.get(customerId);
							StatisticUtils.updateDaily(mapDaily, received_at, secondsRTP);
							mapUserVectorDaily.put(customerId, mapDaily);
						}
						if (processApp) {
							Map<String, Integer> mapApp = mapUserVectorApp.get(customerId) == null
									? new ConcurrentHashMap<>() : mapUserVectorApp.get(customerId);
							StatisticUtils.updateApp(mapApp, appName, secondsRTP);
							mapUserVectorApp.put(customerId, mapApp);
						}
						if (processDays) {
							Map<Integer, Integer> mapDays = mapUserVectorDays.get(customerId) == null
									? new ConcurrentHashMap<>() : mapUserVectorDays.get(customerId);
							StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP, received_at, 27);
							mapUserVectorDays.put(customerId, mapDays);
						}
					}
					if (willProcessCountLogId) {
						countLogId++;
						if (processLogId) {
							setLogId.add(logId);
							Map<String, Integer> mapLogId = mapUserLogIdCount.get(customerId) == null
									? new ConcurrentHashMap<>() : mapUserLogIdCount.get(customerId);
							StatisticUtils.updateLogIdCount(mapLogId, logId);
							mapUserLogIdCount.put(customerId, mapLogId);
						}
					}
				} else {
					prWTF.println(line);
				}
				countTotal++;
				try {
					line = br.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Done process total: " + filePath.split("/")[filePath.split("/").length - 1]
					+ " | Total: " + countTotal + " | validTime: " + countTime + " | validLogId: " + countLogId
					+ " | Time: " + (System.currentTimeMillis() - start));
			PayTVUtils.LOG_INFO.info("Done process total: " + filePath.split("/")[filePath.split("/").length - 1]
					+ " | Total: " + countTotal + " | validTime: " + countTime + " | validLogId: " + countLogId
					+ " | Time: " + (System.currentTimeMillis() - start));
		}

		// });
		// }
		// executorService.shutdown();
		// while (!executorService.isTerminated()) {
		// }
		printUserUsageStatistic(outputFolderPath, processHourly, processDaily, processApp, processLogId, processDays);
		prWTF.close();
	}

	public void calculateVectorDaysBefore(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
			String outputFolderPath, HdfsIO hdfsIOSimple) throws UnsupportedEncodingException, IOException {
		Map<String, Map<Integer, Integer>> mapDays_48 = new ConcurrentHashMap<>();
		for (String customerId : mapUserDateCondition.keySet()) {
			mapDays_48.put(customerId, new ConcurrentHashMap<>());
		}
		ExecutorService executorService = Executors.newFixedThreadPool(4);

		for (final String filePath : listFileLogPath) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
					for (String customerId : mapUserDateCondition.keySet()) {
						mapCheckDupSMM.put(customerId, new HashSet<>());
					}
					Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
					Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
					long start = System.currentTimeMillis();
					int countTotal = 0;
					int countTime = 0;

					BufferedReader br = null;
					String line = null;
					try {
						br = hdfsIOSimple.getReadStream(filePath);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						line = br.readLine();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					while (line != null) {

						String[] arr = line.split(",");
						String customerId = arr[0];
						String logId = arr[2];
						String appName = arr[3];
						Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
						String unparseSMM = arr[6];
						DateTime sessionMainMenu = PayTVUtils.parseSessionMainMenu(arr[6]);
						DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);
						int secondsSMM = 0;
						int secondsRTP = 0;
						boolean willProcessCountLogId = false;
						boolean willProcessSMM = false;
						boolean willProcessRTP = false;
						int dayDuration = 0;
						if (mapUserDateCondition.containsKey(customerId) && received_at != null
								&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
							willProcessCountLogId = true;
							if (logId.equals("12") || logId.equals("18")) {
								if (sessionMainMenu != null) {
									willProcessSMM = StatisticUtils.willProcessSessionMainMenu(customerId, unparseSMM,
											sessionMainMenu, received_at, mapCheckDupSMM, mapCheckValidSMM);
									if (willProcessSMM) {
										secondsSMM = (int) new Duration(sessionMainMenu, received_at)
												.getStandardSeconds();
										if (secondsSMM <= 0 || secondsSMM > 12 * 3600) {
											willProcessSMM = false;
										}
									}
								}
								willProcessCountLogId = willProcessSMM;
							} else if (PayTVUtils.SET_APP_NAME_RTP.contains(appName)
									&& PayTVUtils.SET_LOG_ID_RTP.contains(logId)) {
								willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId, received_at,
										realTimePlaying, mapCheckValidRTP);
								if (willProcessRTP) {
									secondsRTP = (int) Math.round(realTimePlaying);
									if (secondsRTP <= 0 || secondsRTP > 3 * 3600) {
										willProcessRTP = false;
									}
								}
								willProcessCountLogId = willProcessRTP;
							}
						}

						if (willProcessRTP) {
							dayDuration = DateTimeUtils.getDayDuration(received_at,
									mapUserDateCondition.get(customerId).minusDays(5));
							if (dayDuration >= 0 && dayDuration <= 47) {
								Map<Integer, Integer> mapDays = mapDays_48.get(customerId);
								StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP, received_at, 47);
								mapDays_48.put(customerId, mapDays);
								countTime++;
							}

						}
						countTotal++;
						try {
							line = br.readLine();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}
					PayTVUtils.LOG_INFO.info("Done process total: "
							+ filePath.split("/")[filePath.split("/").length - 1] + " | Total: " + countTotal
							+ " |ValidTime: " + countTime + " | Time: " + (System.currentTimeMillis() - start));
					System.out.println("Done process total: " + filePath.split("/")[filePath.split("/").length - 1]
							+ " | Total: " + countTotal + " |ValidTime: " + countTime + " | Time: "
							+ (System.currentTimeMillis() - start));
					try {
						br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}
		PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorDays48.csv"));
		StatisticUtils.printDays48(pr, mapDays_48);
	}

}
