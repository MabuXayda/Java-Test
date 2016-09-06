package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class UserUsage {
	private static Map<String, Map<String, Integer>> mapUserVectorApp;
	private static Map<String, Map<String, Integer>> mapUserVectorDaily;
	private static Map<String, Map<Integer, Integer>> mapUserVectorHourly;
	private static Map<String, Map<String, Integer>> mapUserLogIdCount;
	private static Map<String, Map<Integer, Integer>> mapUserReturnUse;
	private static Map<String, Map<Integer, Integer>> mapUserVectorDays;
	private static Set<String> setLogId;

	public static void main(String[] args) throws IOException {
	}

	public void reset() {
		mapUserVectorHourly = null;
		mapUserVectorDaily = null;
		mapUserVectorApp = null;
		mapUserLogIdCount = null;
		mapUserReturnUse = null;
		mapUserVectorDays = null;
		setLogId = null;
	}

	public Map<String, Map<String, Integer>> getMapUserVectorApp() {
		return mapUserVectorApp;
	}

	public Map<String, Map<Integer, Integer>> getMapUserVectorHourly() {
		return mapUserVectorHourly;
	}

	private void subInitMap(boolean processHourly, boolean processDaily, boolean processApp, boolean processLogId,
			boolean processReturnUse, boolean processDays) {
		if (processHourly) {
			mapUserVectorHourly = new ConcurrentHashMap<>();
		}
		if (processDaily) {
			mapUserVectorDaily = new ConcurrentHashMap<>();
		}
		if (processApp) {
			mapUserVectorApp = new ConcurrentHashMap<>();
		}
		if (processLogId) {
			setLogId = ConcurrentHashMap.newKeySet();
			mapUserLogIdCount = new ConcurrentHashMap<>();
		}
		if (processReturnUse) {
			mapUserReturnUse = new ConcurrentHashMap<>();
		}
		if (processDays) {
			mapUserVectorDays = new ConcurrentHashMap<>();
		}
	}

	public Map<String, String> calculateUserUsageHourly(String filePath, HdfsIO hdfsIOSimple) {
		subInitMap(true, false, true, false, false, false);
		long start = System.currentTimeMillis();
		int countTotal = 0;
		int countTime = 0;
		// int countLogId = 0;
		Map<String, String> mapUserContract = new HashMap<>();
		Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
		BufferedReader br = null;
		try {
			br = hdfsIOSimple.getReadStream(filePath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String line = null;
		try {
			line = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (line != null) {
			String[] arr = line.split(",");
			// if (arr.length == PayTVUtils.NUMBER_OF_FIELDS) {
			String customerId = arr[0];
			String contract = arr[1];
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

			if (StringUtils.isNumeric(customerId) && received_at != null
					&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
				willProcessCountLogId = true;
				if (logId.equals("12") || logId.equals("18")) {
					if (sessionMainMenu != null) {
						willProcessSMM = StatisticUtils.willProcessSessionMainMenu(customerId, unparseSMM,
								sessionMainMenu, received_at, mapCheckDupSMM, mapCheckValidSMM);
						if (willProcessSMM) {
							secondsSMM = (int) new Duration(sessionMainMenu, received_at).getStandardSeconds();
							if (secondsSMM <= 0 || secondsSMM > 12 * 3600) {
								willProcessSMM = false;
							}
						}
					}
					willProcessCountLogId = willProcessSMM;
				} else if (PayTVUtils.SET_APP_NAME_RTP.contains(appName) && PayTVUtils.SET_LOG_ID_RTP.contains(logId)) {
					willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId, received_at, realTimePlaying,
							mapCheckValidRTP);
					if (willProcessRTP) {
						secondsRTP = (int) Math.round(realTimePlaying);
						if (secondsRTP <= 0 || secondsRTP > 3 * 3600) {
							willProcessRTP = false;
						}
					}
					willProcessCountLogId = willProcessRTP;
				}

				if (!mapUserContract.containsKey(customerId)) {
					mapUserVectorHourly.put(customerId, new ConcurrentHashMap<>());
					mapUserVectorApp.put(customerId, new ConcurrentHashMap<>());
					// mapUserLogIdCount.put(customerId, new
					// ConcurrentHashMap<>());
					mapUserContract.put(customerId, contract);
				}
			}

			if (willProcessRTP) {
				countTime++;
				Map<Integer, Integer> mapHourly = mapUserVectorHourly.get(customerId);
				StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
				mapUserVectorHourly.put(customerId, mapHourly);

				Map<String, Integer> mapApp = mapUserVectorApp.get(customerId);
				StatisticUtils.updateApp(mapApp, appName, secondsRTP);
				mapUserVectorApp.put(customerId, mapApp);
			}
			// if (willProcessCountLogId) {
			// countLogId++;
			// setLogId.add(logId);
			// Map<String, Integer> mapLogId =
			// mapUserLogIdCount.get(customerId);
			// StatisticUtils.updateLogIdCount(mapLogId, logId);
			// mapUserLogIdCount.put(customerId, mapLogId);
			// }

			countTotal++;
			// } else {
			// PayTVUtils.LOG_ERROR.error("Parsed log error: " + line);
			// }

			try {
				line = br.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PayTVUtils.LOG_INFO.info("Done calculate user usage: " + filePath.split("/")[filePath.split("/").length - 1]
				+ " | Total: " + countTotal + " | validTime: " + countTime + " | Time: "
				+ (System.currentTimeMillis() - start));
		System.out.println("Done calculate user usage: " + filePath.split("/")[filePath.split("/").length - 1]
				+ " | Total: " + countTotal + " | validTime: " + countTime + " | Time: "
				+ (System.currentTimeMillis() - start));
		return mapUserContract;
	}

	public void processStatistic() throws IOException {
		HdfsIO hdfsIOSimple = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE),
				CommonConfig.get(PayTVConfig.HDFS_SITE));
		Map<String, DateTime> mapUserDateCondition = getMapUserDateConditionLocalStatistic();

		List<String> listFilePathInput2 = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-02-01"), hdfsIOSimple);
		List<String> listFilePathInput3 = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-03-01"), hdfsIOSimple);
		List<String> listFilePathInput4 = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-04-01"), hdfsIOSimple);
		List<String> listFilePathInput5 = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-05-01"), hdfsIOSimple);
		List<String> listFilePathInput6 = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-06-01"), hdfsIOSimple);
		List<String> listFileLogPath = new ArrayList<>();
		listFileLogPath.addAll(listFilePathInput2);
		listFileLogPath.addAll(listFilePathInput3);
		listFileLogPath.addAll(listFilePathInput4);
		listFileLogPath.addAll(listFilePathInput5);
		listFileLogPath.addAll(listFilePathInput6);
		FileUtils.sortListFilePathDateTimeHdfs(listFileLogPath);
		String outputFolderPath = CommonConfig.get(PayTVConfig.MAIN_DIR) + "/days_before";
		FileUtils.createFolder(outputFolderPath);

		long start = System.currentTimeMillis();
		calculateVectorDaysBefore(mapUserDateCondition, listFileLogPath, outputFolderPath, hdfsIOSimple);
		PayTVUtils.LOG_INFO.info("=============== Done job Days 48 at: " + (System.currentTimeMillis() - start));
	}

	private Map<String, DateTime> getMapUserDateConditionLocalStatistic() throws IOException {
		Map<String, DateTime> mapChurn_t2 = UserStatus
				.getMapUserChurnDateCondition(CommonConfig.get(PayTVConfig.SUPPORT_DATA_DIR) + "/userChurn_t2.csv");
		Map<String, DateTime> mapChurn_t3 = UserStatus
				.getMapUserChurnDateCondition(CommonConfig.get(PayTVConfig.SUPPORT_DATA_DIR) + "/userChurn_t3.csv");
		Map<String, DateTime> mapChurn_t4 = UserStatus
				.getMapUserChurnDateCondition(CommonConfig.get(PayTVConfig.SUPPORT_DATA_DIR) + "/userChurn_t4.csv");
		Map<String, DateTime> mapChurn_t5 = UserStatus
				.getMapUserChurnDateCondition(CommonConfig.get(PayTVConfig.SUPPORT_DATA_DIR) + "/userChurn_t5.csv");
		Map<String, DateTime> mapChurn_t6 = UserStatus
				.getMapUserChurnDateCondition(CommonConfig.get(PayTVConfig.SUPPORT_DATA_DIR) + "/userChurn_t6.csv");

		// Map<String, DateTime> mapActive_t6 =
		// UserStatus.getMapUserActiveDateCondition(
		// PayTVUtils.FORMAT_DATE_TIME.parseDateTime("2016-07-01 00:00:00"),
		// UserStatus.getMapUserActive(
		// CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) +
		// "/userActive_t6.csv"));

		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		mapUserDateCondition.putAll(mapChurn_t2);
		mapUserDateCondition.putAll(mapChurn_t3);
		mapUserDateCondition.putAll(mapChurn_t4);
		mapUserDateCondition.putAll(mapChurn_t5);
		mapUserDateCondition.putAll(mapChurn_t6);
		// mapUserDateCondition.putAll(mapActive_t6);

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

	private void subInitMapStatistic(Map<String, DateTime> mapUserDateCondition, boolean processHourly,
			boolean processDaily, boolean processApp, boolean processLogId, boolean processReturnUse,
			boolean processDays) {
		if (processHourly) {
			mapUserVectorHourly = new ConcurrentHashMap<>();
		}
		if (processDaily) {
			mapUserVectorDaily = new ConcurrentHashMap<>();
		}
		if (processApp) {
			mapUserVectorApp = new ConcurrentHashMap<>();
		}
		if (processLogId) {
			setLogId = ConcurrentHashMap.newKeySet();
			mapUserLogIdCount = new ConcurrentHashMap<>();
		}
		if (processReturnUse) {
			mapUserReturnUse = new ConcurrentHashMap<>();
		}
		if (processDays) {
			mapUserVectorDays = new ConcurrentHashMap<>();
		}

		for (String customerId : mapUserDateCondition.keySet()) {
			if (processHourly) {
				mapUserVectorHourly.put(customerId, new ConcurrentHashMap<>());
			}
			if (processDaily) {
				mapUserVectorDaily.put(customerId, new ConcurrentHashMap<>());
			}
			if (processApp) {
				mapUserVectorApp.put(customerId, new ConcurrentHashMap<>());
			}
			if (processLogId) {
				mapUserLogIdCount.put(customerId, new ConcurrentHashMap<>());
			}
			if (processReturnUse) {
				Map<Integer, Integer> mapReuse = new ConcurrentHashMap<>();
				for (int i = 0; i <= 27; i++) {
					mapReuse.put(i, 0);
				}
				mapUserReturnUse.put(customerId, mapReuse);
			}
			if (processDays) {
				mapUserVectorDays.put(customerId, new ConcurrentHashMap<>());
			}
		}
	}

	private void printUserUsageStatistic(String outputFolderPath, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processReturnUse, boolean processDays)
			throws IOException {
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
		if (processReturnUse) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/returnUse.csv"));
			StatisticUtils.printReturnUse(pr, mapUserReturnUse);
		}
		if (processDays) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorDays.csv"));
			StatisticUtils.printDays(pr, mapUserVectorDays);
		}

	}

	private void calculateUserUsageStatistic(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
			String outputFolderPath, HdfsIO hdfsIOSimple, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processReturnUse, boolean processDays)
			throws IOException {

		subInitMapStatistic(mapUserDateCondition, processHourly, processDaily, processApp, processLogId,
				processReturnUse, processDays);

		ExecutorService executorService = Executors.newFixedThreadPool(4);

		for (final String filePath : listFileLogPath) {

			executorService.execute(new Runnable() {
				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int countTotal = 0;
					int countTime = 0;
					int countLogId = 0;

					Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
					for (String customerId : mapUserDateCondition.keySet()) {
						mapCheckDupSMM.put(customerId, new HashSet<>());
					}
					Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
					Map<String, DateTime> mapCheckValidRTP = new HashMap<>();

					BufferedReader br = null;
					try {
						br = hdfsIOSimple.getReadStream(filePath);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					String line = null;
					try {
						line = br.readLine();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

					while (line != null) {
						String[] arr = line.split(",");
						// if (arr.length == PayTVUtils.NUMBER_OF_FIELDS) {
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
							dayDuration = DateTimeUtils.getDayDuration(received_at,
									mapUserDateCondition.get(customerId));
							if (dayDuration >= 0 && dayDuration <= 27) {
								willProcessCountLogId = true;
								if (logId.equals("12") || logId.equals("18")) {
									if (sessionMainMenu != null) {
										willProcessSMM = StatisticUtils.willProcessSessionMainMenu(customerId,
												unparseSMM, sessionMainMenu, received_at, mapCheckDupSMM,
												mapCheckValidSMM);
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
								Map<Integer, Integer> mapHourly = mapUserVectorHourly.get(customerId);
								StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
								mapUserVectorHourly.put(customerId, mapHourly);
							}
							if (processDaily) {
								Map<String, Integer> mapDaily = mapUserVectorDaily.get(customerId);
								StatisticUtils.updateDaily(mapDaily, received_at, secondsRTP);
								mapUserVectorDaily.put(customerId, mapDaily);
							}
							if (processApp) {
								Map<String, Integer> mapApp = mapUserVectorApp.get(customerId);
								StatisticUtils.updateApp(mapApp, appName, secondsRTP);
								mapUserVectorApp.put(customerId, mapApp);
							}
							if (processReturnUse) {
								Map<Integer, Integer> mapReuse = mapUserReturnUse.get(customerId);
								StatisticUtils.updateReturnUse(mapReuse, dayDuration);
								mapUserReturnUse.put(customerId, mapReuse);
							}
							if (processDays) {
								Map<Integer, Integer> mapDays = mapUserVectorDays.get(customerId);
								StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP);
								mapUserVectorDays.put(customerId, mapDays);
							}
						}
						if (willProcessCountLogId) {
							countLogId++;
							if (processLogId) {
								setLogId.add(logId);
								Map<String, Integer> mapLogId = mapUserLogIdCount.get(customerId);
								StatisticUtils.updateLogIdCount(mapLogId, logId);
								mapUserLogIdCount.put(customerId, mapLogId);
							}
						}

						countTotal++;
						// if (countTotal % 1000000 == 0) {
						// System.out.println(filePath.toString() + " |
						// Total: " + countTotal + " | validTime: "
						// + countTime + " | validLogId: " + countLogId);
						// }
						// } else {
						// PayTVUtils.LOG_ERROR.error("Parsed log error: " +
						// line);
						// }
						try {
							line = br.readLine();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					try {
						br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					PayTVUtils.LOG_INFO
							.info("Done process total: " + filePath.split("/")[filePath.split("/").length - 1]
									+ " | Total: " + countTotal + " | validTime: " + countTime + " | validLogId: "
									+ countLogId + " | Time: " + (System.currentTimeMillis() - start));

				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		printUserUsageStatistic(outputFolderPath, processHourly, processDaily, processApp, processLogId,
				processReturnUse, processDays);

	}

	private void calculateVectorDaysBefore(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
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
								StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP);
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
