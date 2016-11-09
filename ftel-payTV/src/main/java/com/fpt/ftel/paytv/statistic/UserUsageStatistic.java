package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
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
	private static String status;

	public static void main(String[] args) throws IOException {
		// UserUsageStatistic userUsage = new UserUsageStatistic();
		UserUsageStatistic.calLatePayScore();
	}

	private void initMap() {
		mapUserVectorHourly = new ConcurrentHashMap<>();
		mapUserVectorDaily = new ConcurrentHashMap<>();
		mapUserVectorApp = new ConcurrentHashMap<>();
		setLogId = ConcurrentHashMap.newKeySet();
		mapUserLogIdCount = new ConcurrentHashMap<>();
		mapUserVectorDays = new ConcurrentHashMap<>();
	}

	public void processCheck() throws IOException {
		HdfsIO hdfsIOSimple = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE),
				CommonConfig.get(PayTVConfig.HDFS_SITE));
		DateTime dateTime = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-06-01");
		List<String> listFile = getListLogPathHdfsStatistic(dateTime.minusMonths(1), hdfsIOSimple);
		Map<String, DateTime> mapUserCondition = new HashMap<>();
		mapUserCondition.put("278738", dateTime);
		for (String key : mapUserCondition.keySet()) {
			System.out.println(key + " | " + mapUserCondition.get(key));
		}
		checkRawLog(mapUserCondition, listFile, hdfsIOSimple);
		System.out.println("DONE");
	}

	public void processStatistic() throws IOException {
		HdfsIO hdfsIOSimple = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE),
				CommonConfig.get(PayTVConfig.HDFS_SITE));
		String totalDir = CommonConfig.get(PayTVConfig.MAIN_DIR) + "/VERY_TOTAL";
		FileUtils.createFolder(totalDir);

		for (int i = 3; i < 10; i++) {
			System.out.println("Start t" + i);
			DateTime dateTime = PayTVUtils.FORMAT_DATE_TIME
					.parseDateTime("2016-" + NumberUtils.get2CharNumber(i) + "-01 00:00:00");
			// mapCont
			Map<String, DateTime> mapUserDateCondition = UserStatus
					.getMapUserDateCondition(totalDir + "/total_t" + i + ".csv", PayTVUtils.FORMAT_DATE_TIME_SIMPLE
							.parseDateTime("2016-" + NumberUtils.get2CharNumber(i + 1) + "-01"));
			// input
			List<String> listFilePathInput = getListLogPathHdfsStatistic(dateTime.minusMonths(1), hdfsIOSimple);
			listFilePathInput.addAll(getListLogPathHdfsStatistic(dateTime, hdfsIOSimple));
			// output
			String outputFolderPath = totalDir + "/feature_t" + i;
			FileUtils.createFolder(outputFolderPath);
			// calculate
			statisticUserUsage(mapUserDateCondition, listFilePathInput, outputFolderPath, hdfsIOSimple, true, true,
					true, false, false);
			statisticUserUsage(mapUserDateCondition, listFilePathInput, outputFolderPath, hdfsIOSimple, false, false,
					false, true, false);
			statisticUserUsage(mapUserDateCondition, listFilePathInput, outputFolderPath, hdfsIOSimple, false, false,
					false, false, true);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DONE T" + i + " |");
		}

	}

	public void getUserChurn() throws IOException {
		Map<String, DateTime> mapChurn = new HashMap<>();
		DateTime beginDate = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-10-01");
		DateTime endDate = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-11-01");
		;
		while (new Duration(beginDate, endDate).getStandardSeconds() > 0) {
			String dateString = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(beginDate);
			URL url = new URL(CommonConfig.get(PayTVConfig.GET_USER_CHURN_API) + dateString);
			String content = IOUtils.toString(url, "UTF-8");
			Set<String> setUserCancel = UserStatus.getSetUserChurnApi(content);
			for (String id : setUserCancel) {
				mapChurn.put(id, beginDate);
			}
			System.out.println("Done date: " + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(beginDate) + " | Churn: "
					+ setUserCancel.size());
			beginDate = beginDate.plusDays(1);
		}

		PrintWriter pr = new PrintWriter(new FileWriter(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/churn_t10.csv"));
		for (String key : mapChurn.keySet()) {
			pr.println(key + "," + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(mapChurn.get(key)));
		}
		pr.close();
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
			pr = new PrintWriter(new File(outputFolderPath + "/reuseInfo.csv"));
			StatisticUtils.printReturnUse(pr, mapUserVectorDays);
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

	private void checkRawLog(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
			HdfsIO hdfsIOSimple) throws IOException {
		Set<String> setNoLog = new HashSet<>();
		setNoLog.add("278738");
		PrintWriter pr278738 = new PrintWriter(new FileWriter(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/278738.csv"));
		for (final String filePath : listFileLogPath) {
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

					if (setNoLog.contains(customerId)) {
						pr278738.println(line);
						count++;
					}
					// if (mapUserDateCondition.containsKey(customerId) &&
					// received_at != null
					// && PayTVUtils.SET_APP_NAME_FULL.contains(appName) &&
					// StringUtils.isNumeric(logId)) {
					// dayDuration =
					// PayTVUtils.getDayDurationFromChurn(received_at,
					// mapUserDateCondition.get(customerId));
					// if (dayDuration >= 0 && dayDuration <= 27) {
					// if (setNoLog.contains(customerId)) {
					// pr278738.println(line);
					// count++;
					// }
					// }
					// }
				} catch (Exception e) {
				}
				line = br.readLine();
			}
			br.close();
			System.out.println("Done file: " + filePath + " | total:" + total + " | count:" + count);
		}
		pr278738.close();
	}

	private void statisticUserUsage(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
			String outputFolderPath, HdfsIO hdfsIOSimple, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processDays) throws IOException {
		initMap();
		for (String id : mapUserDateCondition.keySet()) {
			if (processHourly) {
				if (!mapUserVectorHourly.containsKey(id)) {
					mapUserVectorHourly.put(id, new ConcurrentHashMap<>());
				}
			}
			if (processApp) {
				if (!mapUserVectorApp.containsKey(id)) {
					mapUserVectorApp.put(id, new ConcurrentHashMap<>());
				}
			}
			if (processDaily) {
				if (!mapUserVectorDaily.containsKey(id)) {
					mapUserVectorDaily.put(id, new ConcurrentHashMap<>());
				}
			}
			if (processDays) {
				if (!mapUserVectorDays.containsKey(id)) {
					mapUserVectorDays.put(id, new ConcurrentHashMap<>());
				}
			}
			if (processLogId) {
				if (!mapUserLogIdCount.containsKey(id)) {
					mapUserLogIdCount.put(id, new ConcurrentHashMap<>());
				}
			}
		}

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
						if (arr.length >= 9) {
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
								if (!mapCheckDupSMM.containsKey(customerId)) {
									mapCheckDupSMM.put(customerId, new HashSet<>());
								}
								dayDuration = PayTVUtils.getDayDurationFromChurn(received_at,
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
										willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId,
												received_at, realTimePlaying, mapCheckValidRTP);
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
								if (processDays) {
									Map<Integer, Integer> mapDays = mapUserVectorDays.get(customerId);
									StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP, received_at, 27);
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
					status = "Done process total: " + filePath.split("/")[filePath.split("/").length - 1] + " | Total: "
							+ countTotal + " | validTime: " + countTime + " | validLogId: " + countLogId + " | Time: "
							+ (System.currentTimeMillis() - start);
					System.out.println(status);
					PayTVUtils.LOG_INFO.info(status);
				}

			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}
		printUserUsageStatistic(outputFolderPath, processHourly, processDaily, processApp, processLogId, processDays);
	}

	public static void calLatePayScore() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("/home/tunn/data/tv/changeScore_main.csv"));
		String line = br.readLine();
		Map<String, List<String>> mapProcess = new HashMap<>();
		while (line != null) {
			String[] arr = line.split(",");
			String contract = arr[0];
			List<String> listLog = mapProcess.get(contract) == null ? new ArrayList<>() : mapProcess.get(contract);
			listLog.add(line);
			mapProcess.put(contract, listLog);
			line = br.readLine();
		}
		br.close();
		Map<String, Double> mapScore = new HashMap<>();
		// String contract = "AGD017417";
		for (String contract : mapProcess.keySet()) {
			List<String> listLog = mapProcess.get(contract);
			// for(String log : listLog){
			// System.out.println(log);
			// }
			if (listLog.size() == 1) {
				continue;
			} else {
				String oldStatus = null;
				DateTime oldStatusDate = null;
				DateTime flagLock = null;
				Double score = 0.0;

				for (int i = 0; i < listLog.size(); i++) {
					String[] arr = listLog.get(i).split(",");
					String status = arr[2];
					DateTime date = PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(arr[1]);
					if (status.equals("Huy dich vu")) {
						oldStatus = null;
						oldStatusDate = null;
						flagLock = null;
						score = 0.0;
					}
					// System.out.println("OLD: " + oldStatus);
					// System.out.println("NEW: " + status);
					if (oldStatus == null && !status.equals("CTBDV") && !status.equals("Huy dich vu")) {
						oldStatus = status;
						oldStatusDate = date;
					} else if (oldStatus != null && oldStatus.equals("Binh thuong")) {
						if (status.equals("NVLDTT")) {
							oldStatus = status;
							oldStatusDate = date;
						} else if (status.equals("CTBDV")) {
							break;
						}
					} else if (oldStatus != null && oldStatus.equals("NVLDTT")) {
						if (status.equals("Binh thuong")) {
							int duration;
							if (flagLock != null) {
								score += 2.1;
								duration = (int) new Duration(flagLock, date).getStandardDays();
								score += Math.min(0.1, (0.01 * duration));
								flagLock = null;
								oldStatus = status;
								oldStatusDate = date;
							} else {
								if (i + 1 < listLog.size()) {
									DateTime dateNextNgung = PayTVUtils.FORMAT_DATE_TIME_SIMPLE
											.parseDateTime(listLog.get(i + 1).split(",")[1]);
									int giaHanDur = (int) new Duration(date, dateNextNgung).getStandardHours();
									if (listLog.get(i + 1).split(",")[2].equals("NVLDTT") && giaHanDur < 25) {
										score += 0.0001;
									} else {
										score += 1;
										duration = (int) new Duration(oldStatusDate, date).getStandardDays();
										score += Math.min(0.1, (0.01 * duration));
										oldStatus = status;
										oldStatusDate = date;
									}
								} else {
									score += 1;
									duration = (int) new Duration(oldStatusDate, date).getStandardDays();
									score += Math.min(0.1, (0.01 * duration));
									oldStatus = status;
									oldStatusDate = date;
								}
							}
						} else if (status.equals("CTBDV")) {
							flagLock = date;
							continue;
						}
					}
				}
				mapScore.put(contract, score);

			}
		}
		PrintWriter pr = new PrintWriter(new FileWriter("/home/tunn/data/tv/latePayScore.csv"));
		pr.println("Contract,LatePayScore");
		for (String key : mapScore.keySet()) {
			pr.println(key + "\t" + mapScore.get(key));
			if (key.equals("AGD017417")) {
				System.out.println(key + " | " + mapScore.get(key));
			}
		}
		pr.close();
		System.out.println(mapScore.size());
	}

}
