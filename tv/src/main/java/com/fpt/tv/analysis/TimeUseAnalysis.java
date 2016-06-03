package com.fpt.tv.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.tv.utils.CommonConfig;
import com.fpt.tv.utils.SupportData;
import com.fpt.tv.utils.AnalysisUtils;
import com.fpt.tv.utils.Utils;

public class TimeUseAnalysis {

	private Map<String, DateTime> mapUserDateCondition;
	private List<String> listFileLogPath;
	private CommonConfig cf;

	public void test() throws IOException {
		Map<String, Integer> map = new HashMap<>();
		System.out.println(map.get("a"));
	}

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		Utils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis();

		TimeUseAnalysis timeUseAnalysis = new TimeUseAnalysis();
		timeUseAnalysis.checkUserActiveUsage();

		long end = System.currentTimeMillis();
		System.out.println("DONE: " + (end - star));
	}

	public TimeUseAnalysis() throws IOException {
		cf = CommonConfig.getInstance();
		if (mapUserDateCondition == null) {
			mapUserDateCondition = SupportData.getMapUserDateCondition(
					SupportData.getMapUserActive(cf.get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive.csv"),
					SupportData.getMapUserChurn(cf.get(CommonConfig.SUPPORT_DATA_DIR) + "/userChurn.csv"));
		}
		if (listFileLogPath == null) {
			listFileLogPath = new ArrayList<>();
			Utils.loadListFile(listFileLogPath, new File(cf.get(CommonConfig.PARSED_LOG_DIR)));
		}
	}

	public void checkUserActiveUsage() throws IOException{
		Map<String, DateTime> mapUserActive = SupportData.getMapUserActive(cf.get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive.csv");
		Map<String, DateTime> mapUserActiveDateCondition1 = SupportData.getMapUserActiveDateCondition(SupportData.DATE_TIME_FORMAT_HOUR.parseDateTime("2016-03-31 23:59:59"), mapUserActive);
		Map<String, DateTime> mapUserActiveDateCondition2 = SupportData.getMapUserActiveDateCondition(SupportData.DATE_TIME_FORMAT_HOUR.parseDateTime("2016-03-03 23:59:59"), mapUserActive);
		
		Map<String, Map<String, Integer>> totalMapAppRTP1 = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP1 = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDailyRTP1 = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserActiveDateCondition1.keySet()) {
			totalMapHourlyRTP1.put(customerId, new HashMap<>());
			totalMapDailyRTP1.put(customerId, new HashMap<>());
			totalMapAppRTP1.put(customerId, new HashMap<>());
		}
		
		Map<String, Map<String, Integer>> totalMapAppRTP2 = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP2 = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDailyRTP2 = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserActiveDateCondition2.keySet()) {
			totalMapHourlyRTP2.put(customerId, new HashMap<>());
			totalMapDailyRTP2.put(customerId, new HashMap<>());
			totalMapAppRTP2.put(customerId, new HashMap<>());
		}
		
		Map<String, Map<String, Integer>> totalMapAppRTP = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDailyRTP = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserDateCondition.keySet()) {
			totalMapHourlyRTP.put(customerId, new HashMap<>());
			totalMapDailyRTP.put(customerId, new HashMap<>());
			totalMapAppRTP.put(customerId, new HashMap<>());
		}
		
		ExecutorService executorService= Executors.newFixedThreadPool(3);
		for (final String file : listFileLogPath) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int count = 0;
					int countProcessRTP = 0;
					int countProcessRTP1 = 0;
					int countProcessRTP2 = 0;
					Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
					Map<String, DateTime> mapCheckValidRTP1 = new HashMap<>();
					Map<String, DateTime> mapCheckValidRTP2 = new HashMap<>();
					System.out.println("===> Process file: " + file);

					BufferedReader br = null;
					try {
						br = new BufferedReader(new FileReader(file));
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
					String line = null;
					try {
						line = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					while (line != null) {
						String[] arr = line.split(",");
						if (arr.length == 9) {
							String customerId = arr[0];
							String appName = arr[3];
							Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
							DateTime sessionMainMenu = Utils.parseSessionMainMenu(arr[6]);
							DateTime received_at = Utils.parseReceived_at(arr[8]);
							
							if (mapUserActiveDateCondition1.containsKey(customerId) && sessionMainMenu != null
									&& received_at != null && Utils.LIST_APP_NAME.contains(appName)) {
								long duration = new Duration(received_at, mapUserActiveDateCondition1.get(customerId))
										.getStandardDays();
								if (duration >= 0 && duration <= 27) {
									boolean willProcessRTP = willProcessRealTimePlaying(customerId, received_at,
											realTimePlaying, mapCheckValidRTP1);
									if (willProcessRTP) {
										int seconds = (int) Math.round(realTimePlaying);
										if (seconds > 0 && seconds <= (3 * 3600)) {
											updateAppHourlyDaily(customerId, received_at, appName, seconds,
													totalMapAppRTP1, totalMapHourlyRTP1, totalMapDailyRTP1);
											countProcessRTP1++;
										}
										
									}
								}
							}
							
							if (mapUserActiveDateCondition2.containsKey(customerId) && sessionMainMenu != null
									&& received_at != null && Utils.LIST_APP_NAME.contains(appName)) {
								long duration = new Duration(received_at, mapUserActiveDateCondition2.get(customerId))
										.getStandardDays();
								if (duration >= 0 && duration <= 27) {
									boolean willProcessRTP = willProcessRealTimePlaying(customerId, received_at,
											realTimePlaying, mapCheckValidRTP2);
									if (willProcessRTP) {
										int seconds = (int) Math.round(realTimePlaying);
										if (seconds > 0 && seconds <= (3 * 3600)) {
											updateAppHourlyDaily(customerId, received_at, appName, seconds,
													totalMapAppRTP2, totalMapHourlyRTP2, totalMapDailyRTP2);
											countProcessRTP2++;
										}
									}
								}
							}
							
							if (mapUserDateCondition.containsKey(customerId) && sessionMainMenu != null
									&& received_at != null && Utils.LIST_APP_NAME.contains(appName)) {
								long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
										.getStandardDays();
								if (duration >= 0 && duration <= 27) {
									boolean willProcessRTP = willProcessRealTimePlaying(customerId, received_at,
											realTimePlaying, mapCheckValidRTP);
									if (willProcessRTP) {
										int seconds = (int) Math.round(realTimePlaying);
										if (seconds > 0 && seconds <= (3 * 3600)) {
											updateAppHourlyDaily(customerId, received_at, appName, seconds,
													totalMapAppRTP, totalMapHourlyRTP, totalMapDailyRTP);
											countProcessRTP++;
										}
									}
								}
							}

						} else {
							Utils.LOG_ERROR.error("Parsed log error: " + line);
						}

						try {
							line = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}

						count++;
						if (count % 500000 == 0) {
							System.out.println(file.split("/")[file.split("/").length - 1] + " | " + count);
						}

					}

					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

					Utils.LOG_INFO.info("Done process file: " + file.split("/")[file.split("/").length - 1]
							+ " | 31-3/2-3/Process/Total: " + countProcessRTP1 + "/" + countProcessRTP2 + "/"
							+ countProcessRTP + "/" + count + " | Time: " + (System.currentTimeMillis() - start));
				}
			});

		}

		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}
		
		AnalysisUtils.printApp(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorApp1.csv"),
				totalMapAppRTP1);
		AnalysisUtils.printHourly(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorHourly1.csv"),
				totalMapHourlyRTP1);
		AnalysisUtils.printDaily(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorDaily1.csv"),
				totalMapDailyRTP1);
		AnalysisUtils.printApp(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorApp2.csv"),
				totalMapAppRTP2);
		AnalysisUtils.printHourly(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorHourly2.csv"),
				totalMapHourlyRTP2);
		AnalysisUtils.printDaily(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorDaily2.csv"),
				totalMapDailyRTP2);
		AnalysisUtils.printApp(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorApp.csv"),
				totalMapAppRTP);
		AnalysisUtils.printHourly(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorHourly.csv"),
				totalMapHourlyRTP);
		AnalysisUtils.printDaily(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorDaily.csv"),
				totalMapDailyRTP);

	}
	
	public void getVectorAppHourlyDaily() throws IOException {
//		Map<String, Map<String, Integer>> totalMapAppSMM = Collections.synchronizedMap(new HashMap<>());
//		Map<String, Map<Integer, Integer>> totalMapHourlySMM = Collections.synchronizedMap(new HashMap<>());
//		Map<String, Map<String, Integer>> totalMapDailySMM = Collections.synchronizedMap(new HashMap<>());
//		for (String customerId : mapUserDateCondition.keySet()) {
//			totalMapHourlySMM.put(customerId, new HashMap<>());
//			totalMapDailySMM.put(customerId, new HashMap<>());
//			totalMapAppSMM.put(customerId, new HashMap<>());
//		}

		Map<String, Map<String, Integer>> totalMapAppRTP = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDailyRTP = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserDateCondition.keySet()) {
			totalMapHourlyRTP.put(customerId, new HashMap<>());
			totalMapDailyRTP.put(customerId, new HashMap<>());
			totalMapAppRTP.put(customerId, new HashMap<>());
		}
		// <----------
//		PrintWriter prJoin = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/log.csv"));
		// ---------->
		ExecutorService executorService = Executors.newFixedThreadPool(3);
		for (final String file : listFileLogPath) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int count = 0;
					int countProcessSMM = 0;
					int countProcessRTP = 0;
//					Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
//					for (String customerId : mapUserDateCondition.keySet()) {
//						mapCheckDupSMM.put(customerId, new HashSet<>());
//					}
//					Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
					Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
					System.out.println("===> Process file: " + file);

					BufferedReader br = null;
					try {
						br = new BufferedReader(new FileReader(file));
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					}
					String line = null;
					try {
						line = br.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					// <----------
//					PrintWriter prCheck = null;
//					try {
//						prCheck = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/check/check_"
//								+ file.split("/")[file.split("/").length - 1]));
//					} catch (IOException e1) {
//						e1.printStackTrace();
//					}
					// ---------->
					while (line != null) {
						String[] arr = line.split(",");
						if (arr.length == 9) {
							String customerId = arr[0];
//							String logId = arr[2];
//							String unparseSMM = arr[6];
							String appName = arr[3];
							Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
							DateTime sessionMainMenu = Utils.parseSessionMainMenu(arr[6]);
							DateTime received_at = Utils.parseReceived_at(arr[8]);
							// <----------
//							if (customerId.equals("552353")) {
//								prCheck.println(line);
//								prJoin.println(line);
//							}
							// ---------->
							if (mapUserDateCondition.containsKey(customerId) && sessionMainMenu != null
									&& received_at != null && Utils.LIST_APP_NAME.contains(appName)) {
								long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
										.getStandardDays();
								if (duration >= 0 && duration <= 27) {

//									boolean willProcessSMM = willProcessSessionMainMenu(customerId, logId, unparseSMM,
//											sessionMainMenu, received_at, mapCheckDupSMM, mapCheckValidSMM);
//									if (willProcessSMM) {
//										int seconds = (int) new Duration(sessionMainMenu, received_at)
//												.getStandardSeconds();
//										if (seconds > 0 && seconds <= (12 * 3600)) {
//											updateAppHourlyDaily(customerId, received_at, appName, seconds,
//													totalMapAppSMM, totalMapHourlySMM, totalMapDailySMM);
//											countProcessSMM++;
//										}
//									}

									boolean willProcessRTP = willProcessRealTimePlaying(customerId, received_at,
											realTimePlaying, mapCheckValidRTP);
									if (willProcessRTP) {
										int seconds = (int) Math.round(realTimePlaying);
										if (seconds > 0 && seconds <= (3 * 3600)) {
											updateAppHourlyDaily(customerId, received_at, appName, seconds,
													totalMapAppRTP, totalMapHourlyRTP, totalMapDailyRTP);
											countProcessRTP++;
										}
									}
								}
							}

						} else {
							Utils.LOG_ERROR.error("Parsed log error: " + line);
						}

						try {
							line = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}

						count++;
						if (count % 500000 == 0) {
							System.out.println(file.split("/")[file.split("/").length - 1] + " | " + count);
						}

					}

					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

//					prCheck.close();

					Utils.LOG_INFO.info("Done process file: " + file.split("/")[file.split("/").length - 1]
							+ " | SMM/RTP/Total: " + countProcessSMM + "/" + countProcessRTP + "/" + count + " | Time: "
							+ (System.currentTimeMillis() - start));
				}
				
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

//		AnalysisUtils.printApp(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "SMM_vectorApp.csv"),
//				totalMapAppSMM);
//		AnalysisUtils.printHourly(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "SMM_vectorHourly.csv"),
//				totalMapHourlySMM);
//		AnalysisUtils.printDaily(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "SMM_vectorDaily.csv"),
//				totalMapDailySMM);
		AnalysisUtils.printApp(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorApp.csv"),
				totalMapAppRTP);
		AnalysisUtils.printHourly(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorHourly.csv"),
				totalMapHourlyRTP);
		AnalysisUtils.printDaily(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/RTP_vectorDaily.csv"),
				totalMapDailyRTP);

//		prJoin.close();
	}

	private void updateAppHourlyDaily(String customerId, DateTime stopTime, String appName, int seconds,
			Map<String, Map<String, Integer>> totalMapApp, Map<String, Map<Integer, Integer>> totalMapHourly,
			Map<String, Map<String, Integer>> totalMapDaily) {

		Map<Integer, Integer> mapHourly = totalMapHourly.get(customerId);
		AnalysisUtils.updateHourly(mapHourly, stopTime, seconds);
		totalMapHourly.put(customerId, mapHourly);

		Map<String, Integer> mapDaily = totalMapDaily.get(customerId);
		AnalysisUtils.updateDaily(mapDaily, stopTime, seconds);
		totalMapDaily.put(customerId, mapDaily);

		Map<String, Integer> mapApp = totalMapApp.get(customerId);
		AnalysisUtils.updateApp(mapApp, appName, seconds);
		totalMapApp.put(customerId, mapApp);
	}

	public static boolean willProcessRealTimePlaying(String customerId, DateTime received_at, Double realTimePlaying,
			Map<String, DateTime> mapCheckValidRTP) {
		boolean willProcess = false;
		if (realTimePlaying != null) {
			if (!mapCheckValidRTP.containsKey(customerId)) {
				mapCheckValidRTP.put(customerId, received_at);
				willProcess = true;
			} else if (realTimePlaying < new Duration(mapCheckValidRTP.get(customerId), received_at)
					.getStandardSeconds()) {
				mapCheckValidRTP.put(customerId, received_at);
				willProcess = true;
			}
		}
		return willProcess;
	}

	public static boolean willProcessSessionMainMenu(String customerId, String logId, String unparseSMM,
			DateTime sessionMainMenu, DateTime received_at, Map<String, Set<String>> mapCheckDupSMM,
			Map<String, DateTime> mapCheckValidSMM) {
		boolean willProcess = false;
		if (logId.equals("18") || logId.equals("12")) {
			Set<String> setCheckDupSMM = mapCheckDupSMM.get(customerId);
			if (!setCheckDupSMM.contains(unparseSMM)) {
				setCheckDupSMM.add(unparseSMM);
				mapCheckDupSMM.put(customerId, setCheckDupSMM);
				if (!mapCheckValidSMM.containsKey(customerId)) {
					mapCheckValidSMM.put(customerId, received_at);
					willProcess = true;
				} else if (new Duration(mapCheckValidSMM.get(customerId), sessionMainMenu)
						.getStandardSeconds() > (-60)) {
					mapCheckValidSMM.put(customerId, received_at);
					willProcess = true;
				}
			}
		}
		return willProcess;
	}

}
