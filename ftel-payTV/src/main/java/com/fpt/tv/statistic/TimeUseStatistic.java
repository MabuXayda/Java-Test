package com.fpt.tv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.utils.CommonConfig;
import com.fpt.ftel.core.utils.StatisticUtils;
import com.fpt.ftel.core.utils.SupportData;
import com.fpt.ftel.core.utils.Utils;

public class TimeUseStatistic {


	public static void main(String[] args) throws IOException {
		Utils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis(); 
		TimeUseStatistic timeUseAnalysis = new TimeUseStatistic();
		timeUseAnalysis.process();

		System.out.println("DONE: " + (System.currentTimeMillis() - star));
	}
	
	public void process() throws IOException{
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
		
		List<File> listFile_t2 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2").listFiles());
		List<File> listFile_t3 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t3").listFiles());
		List<File> listFile_t4 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4").listFiles());
		List<File> listFileTrain = Stream.concat(listFile_t2.stream(), listFile_t3.stream()).collect(Collectors.toList());
		List<File> listFileTest = Stream.concat(listFile_t3.stream(), listFile_t4.stream()).collect(Collectors.toList());
		Utils.sortListFile(listFileTrain);
		Utils.sortListFile(listFileTest);
		
		getReuseTime(mapUserDateConditionTest, listFileTest, CommonConfig.getInstance().get(CommonConfig.MAIN_DIR));
	}
	
	public Integer getWeek (long duration){
		if(duration >= 20){
			return 1;
		}else if (duration >= 13) {
			return 2;
		}else if (duration >= 6) {
			return 3;
		}else {
			return 4;
		}
	}

	public void getReuseTime(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath, String ouputReuseTimePath) throws IOException{
		Map<String, Map<Integer, Integer>> mapReuseTime = Collections.synchronizedMap(new HashMap<>());
		for(String customerId : mapUserDateCondition.keySet()){
			Map<Integer, Integer> mapUse = new HashMap<>();
			for(int i = 0; i <=26; i++){
				mapUse.put(i, 0);
			}
			mapReuseTime.put(customerId, mapUse);
		}
		ExecutorService executorService =  Executors.newFixedThreadPool(3);
		for(final File file : listFileLogPath){
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
										boolean willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId,
												received_at, realTimePlaying, mapCheckValidRTP);

										if (willProcessRTP) {
											int seconds = (int) Math.round(realTimePlaying);
											if (seconds > 0 && seconds <= (3 * 3600)) {
												Integer distance = new Integer((int) duration);
												Map<Integer, Integer> mapUse = mapReuseTime.get(customerId);
												mapUse.put(distance, 1);
												countProcessRTP++;
											}
										}
									}
								}
							}
						}

						try {
							line = br.readLine();
						} catch (IOException e) {
							// TODO Auto-generated catch block
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
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Utils.LOG_INFO.info("Done process job reuseTime: " + file.getName() + " | RTP/Total: " + countProcessRTP
							+ "/" + count + " | Time: " + (System.currentTimeMillis() - start));
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}
		
		calculateReuseTime(mapReuseTime, Utils.getPrintWriter(ouputReuseTimePath + "/reuseTime.csv"));
	}
	
	public void calculateReuseTime(Map<String, Map<Integer, Integer>> mapReuseTime, PrintWriter pr){
		pr.println("CustomerId,ReuseCount,ReuseSum,ReuseAvg,ReuseMax");
		for(String customerId : mapReuseTime.keySet()){
			Map<Integer, Integer> mapUse = mapReuseTime.get(customerId);
			int count = 0;
			int sum = 0;
			int max = 0;
			int start = 0;
			
			for (int i = 1; i <= 26; i++) {
				if (i < 26 && mapUse.get(i) == 1) {
					count += 1;
					sum = sum + (i - start);
					max = Math.max(max, i - start);
					start = i;
				} else if (i == 26) {
					sum = sum + (i - start);
					max = Math.max(max, i - start);
				}
			}
			double avg = 0;
			if (count == 0) {
				avg = sum;
			} else {
				avg = sum / (double) count;
			}
			pr.println(customerId + "," + count + "," + sum + "," + avg + "," + max);
		}
		pr.close();
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

										boolean willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId,
												received_at, realTimePlaying, mapCheckValidRTP);
										if (willProcessRTP) {
											int seconds = (int) Math.round(realTimePlaying);
											if (seconds > 0 && seconds <= (3 * 3600)) {

												Map<Integer, Integer> mapHourly = totalMapHourlyRTP.get(customerId);
												StatisticUtils.updateHourly(mapHourly, received_at, seconds);
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

		StatisticUtils.printHourly(Utils.getPrintWriter(outputFolderPath + "/vectorHourly.csv"), totalMapHourlyRTP);
//		AnalysisUtils.printApp(Utils.getPrintWriter(outputFolderPath + "/vectorApp.csv"), totalMapAppRTP,
//				Utils.SET_APP_NAME_RTP);
//		AnalysisUtils.printDaily(Utils.getPrintWriter(outputFolderPath + "/vectorDaily.csv"), totalMapDailyRTP);
		// <----------
		// prJoin.close();
		// ---------->
	}
	
}
