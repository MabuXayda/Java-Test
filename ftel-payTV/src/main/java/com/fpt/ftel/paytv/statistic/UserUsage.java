package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
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

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIOSimple;
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
		long start = System.currentTimeMillis();
		UserUsage usageStatistic = new UserUsage();
		PayTVUtils.LOG_INFO.info("=============== Start job total at: " + start);
		usageStatistic.processStatistic();
		PayTVUtils.LOG_INFO.info("=============== Done job total at: " + (System.currentTimeMillis() - start));
		// usageStatistic.test();

	}

	public UserUsage() {
		PropertyConfigurator.configure("./log4j.properties");
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

	public void test() throws IOException {
		List<String> listFilePathInput = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-06-01"), new HdfsIOSimple());
		System.out.println(listFilePathInput.get(0));
	}
	
	public Map<String, Map<String, Integer>> getMapUserVectorApp(){
		return mapUserVectorApp;
	}
	
	public Map<String, Map<Integer, Integer>> getMapUserVectorHourly(){
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
	
	public Set<String> calculateUserUsageHourly(String filePath, HdfsIOSimple hdfsIOSimple) {
		subInitMap(true, false, true, false, false, false);
		long start = System.currentTimeMillis();
		int countTotal = 0;
		int countTime = 0;
		int countLogId = 0;
		Set<String> setUser = new HashSet<>();
		Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
		BufferedReader br = null;
		try {
			br = hdfsIOSimple.getReadStreamFromHdfs(filePath);
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

				if (StringUtils.isNumeric(customerId) && received_at != null && PayTVUtils.SET_APP_NAME_FULL.contains(appName)
						&& StringUtils.isNumeric(logId)) {
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
					
					if (!setUser.contains(customerId)) {
						mapUserVectorHourly.put(customerId, new ConcurrentHashMap<>());
						mapUserVectorApp.put(customerId, new ConcurrentHashMap<>());
						mapUserLogIdCount.put(customerId, new ConcurrentHashMap<>());
						setUser.add(customerId);
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
				if (willProcessCountLogId) {
					countLogId++;
					setLogId.add(logId);
					Map<String, Integer> mapLogId = mapUserLogIdCount.get(customerId);
					StatisticUtils.updateLogIdCount(mapLogId, logId);
					mapUserLogIdCount.put(customerId, mapLogId);
				}

				countTotal++;
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
		return setUser;
	}
	
	public void processStatistic() throws IOException {
		HdfsIOSimple hdfsIOSimple = new HdfsIOSimple();
		Map<String, DateTime> mapUserDateCondition = getMapUserDateConditionLocalStatistic();
		List<String> listFilePathInput = getListLogPathHdfsStatistic(
				PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime("2016-06-01"), hdfsIOSimple);
		String outputFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_t6";
		FileUtils.createFolder(outputFolderPath);

		long start = System.currentTimeMillis();
		calculateUseUsageStatistic(mapUserDateCondition, listFilePathInput, outputFolderPath, hdfsIOSimple, true, true,
				true, false, true, false);
		PayTVUtils.LOG_INFO
				.info("=============== Done job HourlyDailyAppReturn t3 at: " + (System.currentTimeMillis() - start));
		reset();

		start = System.currentTimeMillis();
		calculateUseUsageStatistic(mapUserDateCondition, listFilePathInput, outputFolderPath, hdfsIOSimple, false, false,
				false, true, false, false);
		PayTVUtils.LOG_INFO.info("=============== Done job LogId t3 at: " + (System.currentTimeMillis() - start));
		reset();

		start = System.currentTimeMillis();
		calculateUseUsageStatistic(mapUserDateCondition, listFilePathInput, outputFolderPath, hdfsIOSimple, false, false,
				false, false, false, true);
		PayTVUtils.LOG_INFO.info("=============== Done job Days t3 at: " + (System.currentTimeMillis() - start));
	}

	private Map<String, DateTime> getMapUserDateConditionLocalStatistic() throws IOException {
		Map<String, DateTime> mapChurn_t6 = UserStatus.getMapUserChurnDateCondition(UserStatus
				.getMapUserChurn(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn_t6.csv"));

		Map<String, DateTime> mapActive_t6 = UserStatus.getMapUserActiveDateCondition(
				PayTVUtils.FORMAT_DATE_TIME.parseDateTime("2016-07-01 00:00:00"), UserStatus.getMapUserActive(
						CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive_t6.csv"));

		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		mapUserDateCondition.putAll(mapChurn_t6);
		mapUserDateCondition.putAll(mapActive_t6);

		return mapUserDateCondition;
	}

	private List<String> getListLogPathLocalStatistic() throws IOException {
		List<String> listFile_t2 = FileUtils
				.getListFilePath(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2"));
		List<String> listFile_t3 = FileUtils
				.getListFilePath(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t3"));
		List<String> listFile_t4 = FileUtils
				.getListFilePath(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4"));
		List<String> listFile_t5 = FileUtils
				.getListFilePath(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t5"));

		List<String> listLogPath = Stream.concat(listFile_t2.stream(), listFile_t3.stream())
				.collect(Collectors.toList());
		FileUtils.sortListFilePathDateTime(listLogPath);
		return listLogPath;
	}

	private List<String> getListLogPathHdfsStatistic(DateTime dateTime, HdfsIOSimple hdfsIOSimple) throws IOException {
		int year = dateTime.getYear();
		int month = dateTime.getMonthOfYear();
		int monthBefore = dateTime.minusMonths(1).getMonthOfYear();
		String path = CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/"
				+ NumberUtils.getTwoCharNumber(monthBefore);
		List<String> listLogPath = hdfsIOSimple.getAllFilePath(path);
		path = CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/"
				+ NumberUtils.getTwoCharNumber(month);
		listLogPath.addAll(hdfsIOSimple.getAllFilePath(path));
		FileUtils.sortListFilePathDateTimeHdfs(listLogPath);
		return listLogPath;
	}

	private void subInitMapStatistic(Map<String, DateTime> mapUserDateCondition, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processReturnUse, boolean processDays) {
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

	private void printUserUsageStatistic(String outputFolderPath, boolean processHourly, boolean processDaily, boolean processApp,
			boolean processLogId, boolean processReturnUse, boolean processDays) throws IOException {
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

	private void calculateUseUsageStatistic(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
			String outputPath, HdfsIOSimple hdfsIOSimple, boolean processHourly, boolean processDaily,
			boolean processApp, boolean processLogId, boolean processReturnUse, boolean processDays)
			throws IOException {

		subInitMapStatistic(mapUserDateCondition, processHourly, processDaily, processApp, processLogId, processReturnUse,
				processDays);

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
						br = hdfsIOSimple.getReadStreamFromHdfs(filePath);
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

		printUserUsageStatistic(outputPath, processHourly, processDaily, processApp, processLogId, processReturnUse, processDays);

	}

}
