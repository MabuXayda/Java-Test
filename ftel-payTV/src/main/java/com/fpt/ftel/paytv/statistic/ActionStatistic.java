package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
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

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class ActionStatistic {

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		PayTVUtils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis();
		ActionStatistic actionAnalysis = new ActionStatistic();
		actionAnalysis.process();


		System.out.println("DONE: " + (System.currentTimeMillis() - star));
	}
	
	public void process() throws IOException{
		Map<String, DateTime> mapUserDateCondition = UserStatus.getMapUserActiveDateCondition(
				PayTVUtils.FORMAT_DATE_TIME_WITH_HOUR.parseDateTime("2016-02-29 00:00:00"), 
				UserStatus.getMapUserActive(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive.csv"));
		
		List<File> listFile_t2 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2").listFiles());
		FileUtils.sortListFileDateTime(listFile_t2);
		
		
		getLogIdCount(mapUserDateCondition, listFile_t2, CommonConfig.getInstance().get(CommonConfig.MAIN_DIR));
	}

	public void getLogIdCount(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputFolderPath) throws IOException {

		Map<String, Map<String, Integer>> totalMapLogId = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserDateCondition.keySet()) {
			totalMapLogId.put(customerId, new HashMap<>());
		}

		Set<String> setLogId = new HashSet<>();

		ExecutorService executorService = Executors.newFixedThreadPool(3);
		for (final File file : listFileLogPath) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int count = 0;
					int countProcessCountLogId = 0;

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
							String logId = arr[2];
							String appName = arr[3];
							Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
							String unparseSMM = arr[6];
							DateTime sessionMainMenu = PayTVUtils.parseSessionMainMenu(arr[6]);
							DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);

							if (mapUserDateCondition.containsKey(customerId) && received_at != null
									&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
								setLogId.add(logId);

								long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
										.getStandardDays();
								if (duration >= 0 && duration <= 26) {

									boolean willProcessCountLogId = true;

									if (logId.equals("12") || logId.equals("18")) {
										if (sessionMainMenu != null) {
											boolean willProcessSMM = StatisticUtils.willProcessSessionMainMenu(
													customerId, unparseSMM, sessionMainMenu, received_at,
													mapCheckDupSMM, mapCheckValidSMM);
											if (willProcessSMM) {
												int secondsSMM = (int) new Duration(sessionMainMenu, received_at)
														.getStandardSeconds();
												if (secondsSMM <= 0 || secondsSMM > 12 * 3600) {
													willProcessSMM = false;
												}
											}
											willProcessCountLogId = willProcessSMM;
										}
									} else if (PayTVUtils.SET_APP_NAME_RTP.contains(appName)
											&& PayTVUtils.SET_LOG_ID_RTP.contains(logId)) {
										boolean willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId,
												received_at, realTimePlaying, mapCheckValidRTP);
										if (willProcessRTP) {
											int secondsRTP = (int) Math.round(realTimePlaying);
											if (secondsRTP <= 0 || secondsRTP > 3 * 3600) {
												willProcessRTP = false;
											}
										}
										willProcessCountLogId = willProcessRTP;
									}

									if (willProcessCountLogId) {
										Map<String, Integer> mapLogId = totalMapLogId.get(customerId);
										StatisticUtils.updateLogIdCount(mapLogId, logId);
										totalMapLogId.put(customerId, mapLogId);

										countProcessCountLogId++;
									}
								}
							}

						} else {
							PayTVUtils.LOG_ERROR.error("Parsed log error: " + line);
						}

						try {
							line = br.readLine();
						} catch (IOException e) {
							e.printStackTrace();
						}

						count++;
						if (count % 500000 == 0) {
							System.out.println(file.getName() + " | " + count);
						}
					}

					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}

					PayTVUtils.LOG_INFO.info("Done process job logId: " + file.getName() + " | CountLogId/Total: "
							+ countProcessCountLogId + "/" + count + " | Time: "
							+ (System.currentTimeMillis() - start));
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}


	}

	public void checkLogIdInAppName(Map<String, DateTime> mapUserDateCondition, List<String> listFileLogPath,
			String outputLogIdCheck) throws IOException {
		Map<String, Set<String>> totalMapLogId = new HashMap<>();
		for (String appName : PayTVUtils.SET_APP_NAME_RTP) {
			totalMapLogId.put(appName, new HashSet<>());
		}
		for (final String file : listFileLogPath) {
			long start = System.currentTimeMillis();
			int count = 0;
			int countProcess = 0;

			Map<String, DateTime> mapCheckValidRTP = new HashMap<>();

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();

			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {

					String customerId = arr[0];
					String logId = arr[2];
					String appName = arr[3];
					Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
					DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);

					if (mapUserDateCondition.containsKey(customerId) && received_at != null
							&& PayTVUtils.SET_APP_NAME_RTP.contains(appName)) {
						long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
								.getStandardDays();
						if (duration >= 0 && duration <= 26) {
							boolean willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId, received_at,
									realTimePlaying, mapCheckValidRTP);
							if (willProcessRTP) {
								int seconds = (int) Math.round(realTimePlaying);
								if (seconds > 0 && seconds <= (3 * 3600)) {
									Set<String> setLogId = totalMapLogId.get(appName);
									setLogId.add(logId);
									totalMapLogId.put(appName, setLogId);
									countProcess++;
								}
							}
						}
					}

				} else {
					PayTVUtils.LOG_ERROR.error("Parsed log error: " + line);
				}

				line = br.readLine();
				count++;
				if (count % 500000 == 0) {
					System.out.println(file.split("/")[file.split("/").length - 1] + " | " + count);
				}

			}

			br.close();
			PayTVUtils.LOG_INFO.info("Done process file: " + file.split("/")[file.split("/").length - 1] + " | Count/Total: "
					+ countProcess + "/" + count + " | Time: " + (System.currentTimeMillis() - start));
		}

		PrintWriter pr = new PrintWriter(new FileWriter(outputLogIdCheck));
		for (String appName : totalMapLogId.keySet()) {
			pr.print(appName);
			for (String logId : totalMapLogId.get(appName)) {
				pr.print("," + logId);
			}
			pr.println();
		}
		pr.close();
	}

}
