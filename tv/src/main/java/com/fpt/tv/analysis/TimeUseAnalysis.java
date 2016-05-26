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

import com.fpt.tv.utils.SupportDataUtils;
import com.fpt.tv.utils.TimeUseUtils;
import com.fpt.tv.utils.Utils;

public class TimeUseAnalysis {

	private Map<String, DateTime> sp_mapUserDateCondition;
	private List<String> listFileLogPath;

	public void test() throws IOException {
		Map<String, Integer> map = new HashMap<>();
		System.out.println(map.get("a"));
	}

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		Utils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis();

		TimeUseAnalysis timeUseAnalysis = new TimeUseAnalysis();
		timeUseAnalysis.getVectorAppHourlyDaily();

		long end = System.currentTimeMillis();
		System.out.println("DONE: " + (end - star));
	}

	public TimeUseAnalysis() throws IOException {
		if (sp_mapUserDateCondition == null) {
			Map<String, Map<String, DateTime>> mapUserHuy = SupportDataUtils.loadMapUserHuy();
			Set<String> setUserActive = SupportDataUtils.loadSetUserActive(mapUserHuy);
			sp_mapUserDateCondition = SupportDataUtils.getMapUserDateCondition("31/03/2016", setUserActive, mapUserHuy);
		}
		if (listFileLogPath == null) {
			listFileLogPath = new ArrayList<>();
			Utils.loadListFile(listFileLogPath, new File(Utils.DIR + "log_parsed"));
		}
	}

	public void getVectorWeek() throws IOException {
		File[] files = new File(Utils.DIR + "log_parsed/t2/").listFiles();
		Map<String, Map<Integer, Integer>> totalMapWeek = Collections.synchronizedMap(new HashMap<>());
		// ExecutorService executorService = Executors.newFixedThreadPool(10);
		for (File file : files) {

			long start = System.currentTimeMillis();
			int count = 0;
			int countError = 0;
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {
					String customerId = arr[0];
					String appName = arr[3];
					if (!customerId.isEmpty() && !appName.isEmpty()) {
						String logId = arr[2];
						if (logId.equals("12") || logId.equals("18")) {
							DateTime sessionMainMenu = null;
							DateTime received_at = null;
							try {
								sessionMainMenu = Utils.parseSessionMainMenu(arr[6]);
								received_at = Utils.parseReceived_at(arr[8]);
							} catch (Exception e) {
							}
							if (sessionMainMenu != null && received_at != null) {
								Duration duration = new Duration(sessionMainMenu, received_at);
								int seconds = (int) duration.getStandardSeconds();
								if (seconds > 0 && seconds <= (12 * 3600)) {
									Map<Integer, Integer> mapWeek = new HashMap<>();
									if (!totalMapWeek.containsKey(customerId)) {
										totalMapWeek.put(customerId, mapWeek);
									}
									mapWeek = totalMapWeek.get(customerId);
									TimeUseUtils.updateWeek(mapWeek, received_at, seconds);
									totalMapWeek.put(customerId, mapWeek);
								}
							}
						}
					}
				} else {
					Utils.LOG_ERROR.error("Parsed log error: " + line);
					countError++;
				}
				count++;
				if (count % 500000 == 0) {
					System.out.println(count);
				}
				line = br.readLine();
			}
			br.close();
			Utils.LOG_INFO.info("Done: " + file.getName() + " | Count: " + count + " | Count error: " + countError
					+ " | Time: " + (System.currentTimeMillis() - start));
		}

		PrintWriter prWeek = new PrintWriter(new FileWriter(Utils.DIR + "vectorWeek.csv"));
		TimeUseUtils.printWeek(prWeek, totalMapWeek);
	}

	public void getVectorAppHourlyDaily() throws IOException {
		Map<String, Map<String, Integer>> totalMapAppSMM = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourlySMM = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDailySMM = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : sp_mapUserDateCondition.keySet()) {
			totalMapHourlySMM.put(customerId, new HashMap<>());
			totalMapDailySMM.put(customerId, new HashMap<>());
			totalMapAppSMM.put(customerId, new HashMap<>());
		}

		Map<String, Map<String, Integer>> totalMapAppRTP = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDailyRTP = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : sp_mapUserDateCondition.keySet()) {
			totalMapHourlyRTP.put(customerId, new HashMap<>());
			totalMapDailyRTP.put(customerId, new HashMap<>());
			totalMapAppRTP.put(customerId, new HashMap<>());
		}
		// <----------
		PrintWriter prJoin = new PrintWriter(new FileWriter(Utils.DIR + "log.csv"));
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
					Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
					for (String customerId : sp_mapUserDateCondition.keySet()) {
						mapCheckDupSMM.put(customerId, new HashSet<>());
					}
					Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
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
					PrintWriter prCheck = null;
					try {
						prCheck = new PrintWriter(new FileWriter(
								Utils.DIR + "check/check_" + file.split("/")[file.split("/").length - 1]));
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					// ---------->
					while (line != null) {
						String[] arr = line.split(",");
						if (arr.length == 9) {
							String customerId = arr[0];
							String logId = arr[2];
							String appName = arr[3];
							Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
							String unparseSMM = arr[6];
							DateTime sessionMainMenu = Utils.parseSessionMainMenu(arr[6]);
							DateTime received_at = Utils.parseReceived_at(arr[8]);
							// <----------
							if (customerId.equals("552353")) {
								prCheck.println(line);
								prJoin.println(line);
							}
							// ---------->
							boolean willProcessSMM = willProcessSessionMainMenu(customerId, appName, logId, unparseSMM,
									sessionMainMenu, received_at, mapCheckDupSMM, mapCheckValidSMM,
									sp_mapUserDateCondition);
							if (willProcessSMM) {
								int seconds = (int) new Duration(sessionMainMenu, received_at).getStandardSeconds();
								if (seconds > 0 && seconds <= (12 * 3600)) {
									updateAppHourlyDaily(customerId, received_at, appName, seconds, totalMapAppSMM,
											totalMapHourlySMM, totalMapDailySMM);
									countProcessSMM++;
								}
							}

							boolean willProcessRTP = willProcessRealTimePlaying(customerId, appName, sessionMainMenu,
									received_at, realTimePlaying, mapCheckValidRTP, sp_mapUserDateCondition);
							if (willProcessRTP) {
								int seconds = (int) Math.round(realTimePlaying);
								if (seconds > 0 && seconds <= (3 * 3600)) {
									updateAppHourlyDaily(customerId, received_at, appName, seconds, totalMapAppRTP,
											totalMapHourlyRTP, totalMapDailyRTP);
									countProcessRTP++;
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

					prCheck.close();

					Utils.LOG_INFO.info("Done process file: " + file.split("/")[file.split("/").length - 1]
							+ " | SMM/RTP/Total: " + countProcessSMM + "/" + countProcessRTP + "/" + count + " | Time: "
							+ (System.currentTimeMillis() - start));

				}
			});

		}

		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		TimeUseUtils.printApp(Utils.getPrintWriter(Utils.DIR + "SMM_vectorApp.csv"), totalMapAppSMM);
		TimeUseUtils.printHourly(Utils.getPrintWriter(Utils.DIR + "SMM_vectorHourly.csv"), totalMapHourlySMM);
		TimeUseUtils.printDaily(Utils.getPrintWriter(Utils.DIR + "SMM_vectorDaily.csv"), totalMapDailySMM);
		TimeUseUtils.printApp(Utils.getPrintWriter(Utils.DIR + "RTP_vectorApp.csv"), totalMapAppRTP);
		TimeUseUtils.printHourly(Utils.getPrintWriter(Utils.DIR + "RTP_vectorHourly.csv"), totalMapHourlyRTP);
		TimeUseUtils.printDaily(Utils.getPrintWriter(Utils.DIR + "RTP_vectorDaily.csv"), totalMapDailyRTP);

		prJoin.close();
	}

	private void updateAppHourlyDaily(String customerId, DateTime stopTime, String appName, int seconds,
			Map<String, Map<String, Integer>> totalMapApp, Map<String, Map<Integer, Integer>> totalMapHourly,
			Map<String, Map<String, Integer>> totalMapDaily) {

		Map<Integer, Integer> mapHourly = totalMapHourly.get(customerId);
		TimeUseUtils.updateHourly(mapHourly, stopTime, seconds);
		totalMapHourly.put(customerId, mapHourly);

		Map<String, Integer> mapDaily = totalMapDaily.get(customerId);
		TimeUseUtils.updateDaily(mapDaily, stopTime, seconds);
		totalMapDaily.put(customerId, mapDaily);

		Map<String, Integer> mapApp = totalMapApp.get(customerId);
		TimeUseUtils.updateApp(mapApp, appName, seconds);
		totalMapApp.put(customerId, mapApp);
	}

	public static boolean willProcessRealTimePlaying(String customerId, String appName, DateTime sessionMainMenu,
			DateTime received_at, Double realTimePlaying, Map<String, DateTime> mapCheckValidRTP,
			Map<String, DateTime> mapUserDateCondition) {
		boolean willProcess = false;
		if (mapUserDateCondition.containsKey(customerId) && Utils.LIST_APP_NAME.contains(appName)
				&& sessionMainMenu != null && received_at != null && realTimePlaying != null) {
			if (new Duration(sessionMainMenu, mapUserDateCondition.get(customerId)).getStandardDays() <= 28) {

				if (!mapCheckValidRTP.containsKey(customerId)) {
					mapCheckValidRTP.put(customerId, received_at);
					willProcess = true;
				} else if (realTimePlaying < new Duration(mapCheckValidRTP.get(customerId), received_at)
						.getStandardSeconds()) {
					mapCheckValidRTP.put(customerId, received_at);
					willProcess = true;
				}
			}
		}
		return willProcess;
	}

	public static boolean willProcessSessionMainMenu(String customerId, String appName, String logId, String unparseSMM,
			DateTime sessionMainMenu, DateTime received_at, Map<String, Set<String>> mapCheckDupSMM,
			Map<String, DateTime> mapCheckValidSMM, Map<String, DateTime> mapUserDateCondition) {
		boolean willProcess = false;
		if (mapUserDateCondition.containsKey(customerId) && Utils.LIST_APP_NAME.contains(appName)
				&& sessionMainMenu != null && received_at != null && (logId.equals("18") || logId.equals("12"))) {
			if (new Duration(sessionMainMenu, mapUserDateCondition.get(customerId)).getStandardDays() <= 28) {

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
		}
		return willProcess;
	}

}
