package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.SupportData;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class UserUsageStatistic {
	private static Map<String, Map<String, Integer>> _mapUserVectorApp;
	private static Map<String, Map<String, Integer>> _mapUserVectorDaily;
	private static Map<String, Map<Integer, Integer>> _mapUserVectorHourly;
	private static Map<String, Map<Integer, Integer>> _mapUserVectorWeek;
	private static Map<String, Map<String, Integer>> _mapUserLogIdCount;
	private static Map<String, Map<Integer, Integer>> _mapUserReturnUse;
	private static Set<String> _setLogId;

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		PayTVUtils.LOG_INFO.info("=============== Start job total at: " + start);
		UserUsageStatistic usageStatistic = new UserUsageStatistic();
		usageStatistic.process();
		PayTVUtils.LOG_INFO.info("=============== Done job total at: " + (System.currentTimeMillis() - start));

	}

	public UserUsageStatistic() {
	}

	private void initMap() {
		_setLogId = new HashSet<>();
		_mapUserVectorHourly = new ConcurrentHashMap<>();
		_mapUserVectorDaily = new ConcurrentHashMap<>();
		_mapUserVectorApp = new ConcurrentHashMap<>();
		_mapUserLogIdCount = new ConcurrentHashMap<>();
		_mapUserReturnUse = new ConcurrentHashMap<>();
		_mapUserVectorWeek = new ConcurrentHashMap<>();
	}

	public void process() throws IOException {
		initMap();
		Map<String, DateTime> mapUserDateConditionTrain = SupportData.getMapUserDateCondition(
				SupportData.getMapUserActive(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive.csv"),
				SupportData.getMapUserChurn(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn.csv"),
				"2016-03-31 00:00:00");

		Map<String, DateTime> mapUserDateConditionTest = SupportData.getMapUserDateCondition(
				SupportData.getMapUserActive(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive_t4.csv"),
				SupportData.getMapUserChurn(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn_t4.csv"),
				"2016-04-30 00:00:00");

		List<File> listFile_t2 = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2").listFiles());
		List<File> listFile_t3 = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t3").listFiles());
		List<File> listFile_t4 = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4").listFiles());
		List<File> listFileTrain = Stream.concat(listFile_t2.stream(), listFile_t3.stream())
				.collect(Collectors.toList());
		List<File> listFileTest = Stream.concat(listFile_t3.stream(), listFile_t4.stream())
				.collect(Collectors.toList());
		FileUtils.sortListFile(listFileTrain);
		FileUtils.sortListFile(listFileTest);

		String trainFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_total_train";
		String testFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_total_test";
		FileUtils.createFolder(trainFolderPath);
		FileUtils.createFolder(testFolderPath);

		getUseUsageStatistic(mapUserDateConditionTest, listFileTest, true, true, true, true, true, true);
		printResult(testFolderPath, true, true, true, true, true, true);

	}

	private void subInitMap(Map<String, DateTime> mapUserDateCondition, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processReturnUse, boolean processWeek) {
		for (String customerId : mapUserDateCondition.keySet()) {
			if (processHourly) {
				_mapUserVectorHourly.put(customerId, new ConcurrentHashMap<>());
			}
			if (processDaily) {
				_mapUserVectorDaily.put(customerId, new ConcurrentHashMap<>());
			}
			if (processApp) {
				_mapUserVectorApp.put(customerId, new ConcurrentHashMap<>());
			}
			if (processLogId) {
				_mapUserLogIdCount.put(customerId, new ConcurrentHashMap<>());
			}
			if (processReturnUse) {
				Map<Integer, Integer> mapReuse = new ConcurrentHashMap<>();
				for (int i = 0; i <= 26; i++) {
					mapReuse.put(i, 0);
				}
				_mapUserReturnUse.put(customerId, mapReuse);
			}
			if (processWeek) {
				_mapUserVectorWeek.put(customerId, Collections.synchronizedMap(new ConcurrentHashMap<>()));
			}
		}
	}

	private void printResult(String outputFolderPath, boolean processHourly, boolean processDaily, boolean processApp,
			boolean processLogId, boolean processReturnUse, boolean processWeek) throws IOException {
		if (processHourly) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorHourly.csv"));
			StatisticUtils.printHourly(pr, _mapUserVectorHourly);
		}
		if (processDaily) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorDaily.csv"));
			StatisticUtils.printDaily(pr, _mapUserVectorDaily);
		}
		if (processApp) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorApp.csv"));
			StatisticUtils.printApp(pr, _mapUserVectorApp, PayTVUtils.SET_APP_NAME_RTP);
		}
		if (processLogId) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/logIdCount.csv"));
			StatisticUtils.printLogIdCount(pr, _setLogId, _mapUserLogIdCount);
		}
		if (processReturnUse) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/returnUse.csv"));
			StatisticUtils.printReturnUse(pr, _mapUserReturnUse);
		}
		if (processWeek) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorWeek.csv"));
			StatisticUtils.printWeek(pr, _mapUserVectorWeek);
		}

	}

	private void getUseUsageStatistic(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			boolean processHourly, boolean processDaily, boolean processApp, boolean processLogId,
			boolean processReturnUse, boolean processWeek) throws IOException {

		subInitMap(mapUserDateCondition, processHourly, processDaily, processApp, processLogId, processReturnUse,
				processWeek);

		ExecutorService executorService = Executors.newFixedThreadPool(3);

		for (final File file : listFileLogPath) {

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
						br = new BufferedReader(new FileReader(file));
					} catch (FileNotFoundException e) {
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
							Long dayDuration = null;

							if (mapUserDateCondition.containsKey(customerId) && received_at != null
									&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
								_setLogId.add(logId);
								dayDuration = new Duration(received_at, mapUserDateCondition.get(customerId))
										.getStandardDays();

								if (dayDuration >= 0 && dayDuration <= 26) {
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
									Map<Integer, Integer> mapHourly = _mapUserVectorHourly.get(customerId);
									StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
									_mapUserVectorHourly.put(customerId, mapHourly);
								}
								if (processDaily) {
									Map<String, Integer> mapDaily = _mapUserVectorDaily.get(customerId);
									StatisticUtils.updateDaily(mapDaily, received_at, secondsRTP);
									_mapUserVectorDaily.put(customerId, mapDaily);
								}
								if (processApp) {
									Map<String, Integer> mapApp = _mapUserVectorApp.get(customerId);
									StatisticUtils.updateApp(mapApp, appName, secondsRTP);
									_mapUserVectorApp.put(customerId, mapApp);
								}
								if (processReturnUse) {
									Map<Integer, Integer> mapReuse = _mapUserReturnUse.get(customerId);
									StatisticUtils.updateReturnUse(mapReuse, dayDuration);
									_mapUserReturnUse.put(customerId, mapReuse);
								}
								if (processWeek) {
									Map<Integer, Integer> mapWeek = _mapUserVectorWeek.get(customerId);
									StatisticUtils.updateWeek(mapWeek, DateTimeUtils.getWeekIndexFromDuration(dayDuration),
											secondsRTP);
									_mapUserVectorWeek.put(customerId, mapWeek);
								}
							}
							if (willProcessCountLogId) {
								countLogId++;
								if (processLogId) {
									Map<String, Integer> mapLogId = _mapUserLogIdCount.get(customerId);
									StatisticUtils.updateLogIdCount(mapLogId, logId);
									_mapUserLogIdCount.put(customerId, mapLogId);
								}
							}

							countTotal++;
							if (countTotal % 1000000 == 0) {
								System.out.println("process: " + file.getName() + " | Total: " + countTotal
										+ " | validTime: " + countTime + " | validLogId: " + countLogId);
							}
						} else {
							PayTVUtils.LOG_ERROR.error("Parsed log error: " + line);
						}
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
					PayTVUtils.LOG_INFO.info("Done process total: " + file.getName() + " | Total: " + countTotal
							+ " | validTime: " + countTime + " | validLogId: " + countLogId + " | Time: "
							+ (System.currentTimeMillis() - start));

				}
			});
		}
		executorService.shutdown();
		while (executorService.isTerminated()) {
		}

	}

}
