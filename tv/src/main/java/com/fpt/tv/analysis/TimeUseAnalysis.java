package com.fpt.tv.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.tv.utils.AnalysisUtils;
import com.fpt.tv.utils.CommonConfig;
import com.fpt.tv.utils.SupportData;
import com.fpt.tv.utils.Utils;

public class TimeUseAnalysis {


	public static void main(String[] args) throws IOException {
		Utils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis();
		TimeUseAnalysis timeUseAnalysis = new TimeUseAnalysis();
		timeUseAnalysis.process();

		System.out.println("DONE: " + (System.currentTimeMillis() - star));
	}
	
	public void process() throws IOException{
		Map<String, DateTime> mapUserDateCondition = SupportData.getMapUserActiveDateCondition(
				Utils.DATE_TIME_FORMAT_WITH_HOUR.parseDateTime("2016-02-29 00:00:00"), 
				SupportData.getMapUserActive(CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/userActive.csv"));
		
		List<File> listFile_t2 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2").listFiles());
		Utils.sortListFile(listFile_t2);
		getVectorAppHourlyDaily(mapUserDateCondition, listFile_t2, CommonConfig.getInstance().get(CommonConfig.MAIN_DIR));
	}

	public void getReuseTime(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputReuseTime) throws IOException {
		Map<String, DateTime> mapReturnUsePoint = new HashMap<>();
		Map<String, Integer> mapReturnUseCount = new HashMap<>();
		Map<String, Integer> mapReturnUseSum = new HashMap<>();
		Map<String, Integer> mapReturnUseMax = new HashMap<>();
		Map<String, Integer> mapRTPTotalCount = new HashMap<>();
		for (String customerId : mapUserDateCondition.keySet()) {
			mapReturnUseCount.put(customerId, 0);
		}

		for (final File file : listFileLogPath) {
			long start = System.currentTimeMillis();
			int count = 0;
			int countProcessRTP = 0;
			Map<String, DateTime> mapCheckValidRTP = new HashMap<>();

			System.out.println("===> Process file: " + file);
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();

			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {
					String customerId = arr[0];
					String appName = arr[3];
					Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
					DateTime received_at = Utils.parseReceived_at(arr[8]);
					String logId = arr[2];

					if (mapUserDateCondition.containsKey(customerId) && received_at != null
							&& Utils.SET_APP_NAME_FULL.contains(appName) && Utils.isNumeric(logId)) {

						long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
								.getStandardDays();
						if (duration >= 0 && duration <= 26) {

							if (Utils.SET_APP_NAME_RTP.contains(appName) && Utils.SET_LOG_ID_RTP.contains(logId)) {
								boolean willProcessRTP = AnalysisUtils.willProcessRealTimePlaying(customerId,
										received_at, realTimePlaying, mapCheckValidRTP);

								if (willProcessRTP) {
									int seconds = (int) Math.round(realTimePlaying);
									if (seconds > 0 && seconds <= (3 * 3600)) {
										Utils.addMapKeyStrValInt(mapRTPTotalCount, customerId, 1);
										AnalysisUtils.updateReturnUse(customerId, received_at, seconds,
												mapReturnUsePoint, mapReturnUseCount, mapReturnUseSum, mapReturnUseMax);
										countProcessRTP++;
									}
								}
							}
						}
					}
				}

				line = br.readLine();
				count++;
				if (count % 500000 == 0) {
					System.out.println(file.getName() + " | " + count);
				}
			}

			br.close();
			Utils.LOG_INFO.info("Done process job reuseTime: " + file.getName() + " | RTP/Total: " + countProcessRTP
					+ "/" + count + " | Time: " + (System.currentTimeMillis() - start));
		}

		AnalysisUtils.printReturnUse(Utils.getPrintWriter(outputReuseTime), mapUserDateCondition, mapRTPTotalCount,
				mapReturnUseCount, mapReturnUseSum, mapReturnUseMax);

	}

	public void getVectorAppHourlyDaily(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputFolderPath) throws IOException {
		
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP = Collections.synchronizedMap(new HashMap<>());
//		Map<String, Map<String, Integer>> totalMapAppRTP = Collections.synchronizedMap(new HashMap<>());
//		Map<String, Map<String, Integer>> totalMapDailyRTP = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserDateCondition.keySet()) {
			totalMapHourlyRTP.put(customerId, new HashMap<>());
//			totalMapDailyRTP.put(customerId, new HashMap<>());
//			totalMapAppRTP.put(customerId, new HashMap<>());
		}
		// <----------
//		 PrintWriter prJoin = new PrintWriter(new
//		 FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/log.csv"));
		// ---------->
		ExecutorService executorService = Executors.newFixedThreadPool(3);
		for (final File file : listFileLogPath) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int count = 0;
					int countProcessRTP = 0;
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
//						prCheck = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/log_check/check_"
//								+ file.split("/")[file.split("/").length - 1]));
//					} catch (IOException e1) {
//						e1.printStackTrace();
//					}
					// ---------->
					while (line != null) {
						String[] arr = line.split(",");
						if (arr.length == 9) {
							String customerId = arr[0];
							String appName = arr[3];
							Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
							DateTime received_at = Utils.parseReceived_at(arr[8]);
							String logId = arr[2];
							// <----------
//							if (customerId.equals("442256")) {
//								prCheck.println(line);
//								prJoin.println(line);
//							}
							// ---------->
							if (mapUserDateCondition.containsKey(customerId) && received_at != null
									&& Utils.SET_APP_NAME_FULL.contains(appName) && Utils.isNumeric(logId)) {
								
								long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
										.getStandardDays();
								if (duration >= 0 && duration <= 26) {
									if (Utils.SET_APP_NAME_RTP.contains(appName)
											&& Utils.SET_LOG_ID_RTP.contains(logId)) {

										boolean willProcessRTP = AnalysisUtils.willProcessRealTimePlaying(customerId,
												received_at, realTimePlaying, mapCheckValidRTP);
										if (willProcessRTP) {
											int seconds = (int) Math.round(realTimePlaying);
											if (seconds > 0 && seconds <= (3 * 3600)) {

												Map<Integer, Integer> mapHourly = totalMapHourlyRTP.get(customerId);
												AnalysisUtils.updateHourly(mapHourly, received_at, seconds);
												totalMapHourlyRTP.put(customerId, mapHourly);

//												Map<String, Integer> mapDaily = totalMapDailyRTP.get(customerId);
//												AnalysisUtils.updateDaily(mapDaily, received_at, seconds);
//												totalMapDailyRTP.put(customerId, mapDaily);
//
//												Map<String, Integer> mapApp = totalMapAppRTP.get(customerId);
//												AnalysisUtils.updateApp(mapApp, appName, seconds);
//												totalMapAppRTP.put(customerId, mapApp);

												countProcessRTP++;
											}
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
							System.out.println(file.getName() + " | " + count);
						}

					}

					try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					// <----------
					// prCheck.close();
					// ---------->
					Utils.LOG_INFO.info("Done process job vector: " + file.getName() + " | RTP/Total: "
							+ countProcessRTP + "/" + count + " | Time: " + (System.currentTimeMillis() - start));
				}

			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		AnalysisUtils.printHourly(Utils.getPrintWriter(outputFolderPath + "/vectorHourly.csv"), totalMapHourlyRTP);
//		AnalysisUtils.printApp(Utils.getPrintWriter(outputFolderPath + "/vectorApp.csv"), totalMapAppRTP,
//				Utils.SET_APP_NAME_RTP);
//		AnalysisUtils.printDaily(Utils.getPrintWriter(outputFolderPath + "/vectorDaily.csv"), totalMapDailyRTP);
		// <----------
		// prJoin.close();
		// ---------->
	}
	
}
