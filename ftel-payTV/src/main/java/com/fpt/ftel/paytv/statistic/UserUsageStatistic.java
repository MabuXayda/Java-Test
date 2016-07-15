package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
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
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class UserUsageStatistic {
	private static Map<String, Map<String, Integer>> _mapUserVectorApp;
	private static Map<String, Map<String, Integer>> _mapUserVectorDaily;
	private static Map<String, Map<Integer, Integer>> _mapUserVectorHourly;
	private static Map<String, Map<String, Integer>> _mapUserLogIdCount;
	private static Map<String, Map<Integer, Integer>> _mapUserReturnUse;
	private static Map<String, Map<Integer, Integer>> _mapUserVectorDays;
	private static Set<String> _setLogId;

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		PayTVUtils.LOG_INFO.info("=============== Start job total at: " + start);
		UserUsageStatistic usageStatistic = new UserUsageStatistic();
		usageStatistic.process();
		// usageStatistic.test();
		PayTVUtils.LOG_INFO.info("=============== Done job total at: " + (System.currentTimeMillis() - start));

	}
	
	public void reset(){
		_mapUserVectorHourly = null;
		_mapUserVectorDaily = null;
		_mapUserVectorApp = null;
		_mapUserLogIdCount = null;
		_mapUserReturnUse = null;
		_mapUserVectorDays = null;
		_setLogId = null;
	}

	public void test() {
		DateTime x1 = PayTVUtils.FORMAT_DATE_TIME_WITH_HOUR.parseDateTime("2016-05-01 00:00:00");
		DateTime x2 = PayTVUtils.FORMAT_DATE_TIME_WITH_HOUR.parseDateTime("2016-04-30 05:00:00");
		System.out.println(DateTimeUtils.getDayDuration(x2, x1));
	}

	public void process() throws IOException {
		Map<String, DateTime> mapChurn_t2 = UserStatus.getMapUserChurnDateCondition(UserStatus
				.getMapUserChurn(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn_t2.csv"));
		Map<String, DateTime> mapChurn_t3 = UserStatus.getMapUserChurnDateCondition(UserStatus
				.getMapUserChurn(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn_t3.csv"));
		Map<String, DateTime> mapChurn_t4 = UserStatus.getMapUserChurnDateCondition(UserStatus
				.getMapUserChurn(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn_t4.csv"));
		// Map<String, DateTime> mapChurn_t5 =
		// UserStatus.getMapUserChurnDateCondition(UserStatus
		// .getMapUserChurn(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR)
		// + "/userChurn_t5.csv"));

		Map<String, DateTime> mapActive_t3 = UserStatus.getMapUserActiveDateCondition(
				PayTVUtils.FORMAT_DATE_TIME_WITH_HOUR.parseDateTime("2016-04-01 00:00:00"), UserStatus.getMapUserActive(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive_t3.csv"));
		Map<String, DateTime> mapActive_t4 = UserStatus.getMapUserActiveDateCondition(
				PayTVUtils.FORMAT_DATE_TIME_WITH_HOUR.parseDateTime("2016-05-01 00:00:00"), UserStatus.getMapUserActive(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive_t4.csv"));
		// Map<String, DateTime> mapActive_t5 =
		// UserStatus.getMapUserActiveDateCondition(
		// PayTVUtils.FORMAT_DATE_TIME_WITH_HOUR.parseDateTime("2016-06-01
		// 00:00:00"), UserStatus.getMapUserActive(
		// CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) +
		// "/userActive_t5.csv"));

		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		mapUserDateCondition.putAll(mapActive_t3);
		mapUserDateCondition.putAll(mapChurn_t2);
		mapUserDateCondition.putAll(mapChurn_t3);

		List<File> listFile_t2 = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2").listFiles());
		List<File> listFile_t3 = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t3").listFiles());
		List<File> listFile_t4 = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4").listFiles());
		// List<File> listFile_t5 = Arrays
		// .asList(new
		// File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) +
		// "/t5").listFiles());
		List<File> listFileInput = Stream.concat(listFile_t2.stream(), listFile_t3.stream())
				.collect(Collectors.toList());
		FileUtils.sortListFileDateTime(listFileInput);

		String outputFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_test_t3";
		FileUtils.createFolder(outputFolderPath);
		
		long start = System.currentTimeMillis();
		getUseUsageStatistic(mapUserDateCondition, listFileInput, outputFolderPath, true, true, true, false, true,
				false);
		PayTVUtils.LOG_INFO.info("=============== Done job HourlyDailyAppReturn t3 at: " + (System.currentTimeMillis() - start));
		reset();
		start = System.currentTimeMillis();
		getUseUsageStatistic(mapUserDateCondition, listFileInput, outputFolderPath, false, false, false, true, false,
				false);
		PayTVUtils.LOG_INFO.info("=============== Done job LogId t3 at: " + (System.currentTimeMillis() - start));
		reset();
		start = System.currentTimeMillis();
		getUseUsageStatistic(mapUserDateCondition, listFileInput, outputFolderPath, false, false, false, false, false,
				true);
		PayTVUtils.LOG_INFO.info("=============== Done job Days t3 at: " + (System.currentTimeMillis() - start));
		reset();
		
		mapUserDateCondition = new HashMap<>();
		mapUserDateCondition.putAll(mapActive_t4);
		mapUserDateCondition.putAll(mapChurn_t4);
		listFileInput = Stream.concat(listFile_t3.stream(), listFile_t4.stream())
				.collect(Collectors.toList());
		outputFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_test_t4";
		FileUtils.createFolder(outputFolderPath);
		
		start = System.currentTimeMillis();
		getUseUsageStatistic(mapUserDateCondition, listFileInput, outputFolderPath, true, true, true, false, true,
				false);
		PayTVUtils.LOG_INFO.info("=============== Done job HourlyDailyAppReturn t4 at: " + (System.currentTimeMillis() - start));
		reset();
		start = System.currentTimeMillis();
		getUseUsageStatistic(mapUserDateCondition, listFileInput, outputFolderPath, false, false, false, true, false,
				false);
		PayTVUtils.LOG_INFO.info("=============== Done job LogId t4 at: " + (System.currentTimeMillis() - start));
		reset();
		start = System.currentTimeMillis();
		getUseUsageStatistic(mapUserDateCondition, listFileInput, outputFolderPath, false, false, false, false, false,
				true);
		PayTVUtils.LOG_INFO.info("=============== Done job Days t4 at: " + (System.currentTimeMillis() - start));
	}

	private void subInitMap(Map<String, DateTime> mapUserDateCondition, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processReturnUse, boolean processDays) {
		if (processHourly) {
			_mapUserVectorHourly = new ConcurrentHashMap<>();
		}
		if (processDaily) {
			_mapUserVectorDaily = new ConcurrentHashMap<>();
		}
		if (processApp) {
			_mapUserVectorApp = new ConcurrentHashMap<>();
		}
		if (processLogId) {
			_setLogId = ConcurrentHashMap.newKeySet();
			_mapUserLogIdCount = new ConcurrentHashMap<>();
		}
		if (processReturnUse) {
			_mapUserReturnUse = new ConcurrentHashMap<>();
		}
		if (processDays) {
			_mapUserVectorDays = new ConcurrentHashMap<>();
		}

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
				for (int i = 0; i <= 27; i++) {
					mapReuse.put(i, 0);
				}
				_mapUserReturnUse.put(customerId, mapReuse);
			}
			if (processDays) {
				_mapUserVectorDays.put(customerId, new ConcurrentHashMap<>());
			}
		}
	}

	private void printResult(String outputFolderPath, boolean processHourly, boolean processDaily, boolean processApp,
			boolean processLogId, boolean processReturnUse, boolean processDays) throws IOException {
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
		if (processDays) {
			PrintWriter pr = new PrintWriter(new File(outputFolderPath + "/vectorDays.csv"));
			StatisticUtils.printDays(pr, _mapUserVectorDays);
		}

	}

	private void getUseUsageStatistic(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputPath, boolean processHourly, boolean processDaily, boolean processApp, boolean processLogId,
			boolean processReturnUse, boolean processDays) throws IOException {

		subInitMap(mapUserDateCondition, processHourly, processDaily, processApp, processLogId, processReturnUse,
				processDays);

		ExecutorService executorService = Executors.newFixedThreadPool(4);

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
								if (processDays) {
									Map<Integer, Integer> mapDays = _mapUserVectorDays.get(customerId);
									StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP);
									_mapUserVectorDays.put(customerId, mapDays);
								}
							}
							if (willProcessCountLogId) {
								countLogId++;
								if (processLogId) {
									_setLogId.add(logId);
									Map<String, Integer> mapLogId = _mapUserLogIdCount.get(customerId);
									StatisticUtils.updateLogIdCount(mapLogId, logId);
									_mapUserLogIdCount.put(customerId, mapLogId);
								}
							}

							countTotal++;
							if (countTotal % 1000000 == 0) {
								System.out.println(file.getName() + " | Total: " + countTotal + " | validTime: "
										+ countTime + " | validLogId: " + countLogId);
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
		while (!executorService.isTerminated()) {
		}

		printResult(outputPath, processHourly, processDaily, processApp, processLogId, processReturnUse, processDays);

	}

	private void getUseUsageStatisticHdfs(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputPath, boolean processHourly, boolean processDaily, boolean processApp, boolean processLogId,
			boolean processReturnUse, boolean processDays) throws IOException {

		subInitMap(mapUserDateCondition, processHourly, processDaily, processApp, processLogId, processReturnUse,
				processDays);

		ExecutorService executorService = Executors.newFixedThreadPool(4);

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
								if (processDays) {
									Map<Integer, Integer> mapDays = _mapUserVectorDays.get(customerId);
									StatisticUtils.updateDays(mapDays, dayDuration, secondsRTP);
									_mapUserVectorDays.put(customerId, mapDays);
								}
							}
							if (willProcessCountLogId) {
								countLogId++;
								if (processLogId) {
									_setLogId.add(logId);
									Map<String, Integer> mapLogId = _mapUserLogIdCount.get(customerId);
									StatisticUtils.updateLogIdCount(mapLogId, logId);
									_mapUserLogIdCount.put(customerId, mapLogId);
								}
							}

							countTotal++;
							if (countTotal % 1000000 == 0) {
								System.out.println(file.getName() + " | Total: " + countTotal + " | validTime: "
										+ countTime + " | validLogId: " + countLogId);
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
		while (!executorService.isTerminated()) {
		}

		printResult(outputPath, processHourly, processDaily, processApp, processLogId, processReturnUse, processDays);

	}

}
