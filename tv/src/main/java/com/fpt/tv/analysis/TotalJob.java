package com.fpt.tv.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.tv.utils.AnalysisUtils;
import com.fpt.tv.utils.CommonConfig;
import com.fpt.tv.utils.SupportData;
import com.fpt.tv.utils.Utils;

public class TotalJob {
	public static void main(String[] args) throws IOException {
		System.out.println("START");
		Utils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis();

		TotalJob totalJob = new TotalJob();
		totalJob.process();

		System.out.println("DONE: " + (System.currentTimeMillis() - star));
	}
	
	public static void test(){
		List<File> listFile_t2 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t2").listFiles());
		List<File> listFile_t3 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t3").listFiles());
		List<File> listFile_t4 = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4").listFiles());
		List<File> listFileTrain = Stream.concat(listFile_t2.stream(), listFile_t3.stream()).collect(Collectors.toList());
		List<File> listFileTest = Stream.concat(listFile_t3.stream(), listFile_t4.stream()).collect(Collectors.toList());
		Utils.sortListFile(listFileTrain);
		Utils.sortListFile(listFileTest);
	}

	public void process() throws IOException {
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
		
		String trainFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_train";
		String testFolderPath = CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_test";
		Utils.createFolder(trainFolderPath);
		Utils.createFolder(testFolderPath);
		
		long start = System.currentTimeMillis();
		System.out.println(listFileTrain.size());
		summarizeAnalysis(mapUserDateConditionTrain, listFileTrain, trainFolderPath);
		Utils.LOG_INFO.info("===> Done process train data" + " | Time: " + (System.currentTimeMillis() - start));
		
		start = System.currentTimeMillis();
		System.out.println(listFileTest.size());
		summarizeAnalysis(mapUserDateConditionTest, listFileTest, testFolderPath);
		Utils.LOG_INFO.info("===> Done process test data" + " | Time: " + (System.currentTimeMillis() - start));
		

	}

	public void summarizeAnalysis(Map<String, DateTime> mapUserDateCondition, List<File> listFileLogPath,
			String outputFolderPath) throws IOException {

		Map<String, Map<String, Integer>> totalMapAppRTP = new HashMap<>();
		Map<String, Map<Integer, Integer>> totalMapHourlyRTP = new HashMap<>();
		Map<String, Map<String, Integer>> totalMapDailyRTP = new HashMap<>();
		Map<String, Map<String, Integer>> totalMapLogId = new HashMap<>();
		Map<String, Integer> mapRTPTotalCount = new HashMap<>(); 
		for (String customerId : mapUserDateCondition.keySet()) {
			totalMapAppRTP.put(customerId, new HashMap<>());
			totalMapHourlyRTP.put(customerId, new HashMap<>());
			totalMapDailyRTP.put(customerId, new HashMap<>());
			totalMapLogId.put(customerId, new HashMap<>());
			mapRTPTotalCount.put(customerId, 0);
		}
		Map<String, DateTime> mapReturnUsePoint = new HashMap<>();
		Map<String, Integer> mapReturnUseCount = new HashMap<>();
		Map<String, Integer> mapReturnUseSum = new HashMap<>();
		Map<String, Integer> mapReturnUseMax = new HashMap<>();

		Set<String> setLogId = new HashSet<>();

		for (final File file : listFileLogPath) {
			long start = System.currentTimeMillis();
			int count = 0;
			int countProcess = 0;

			Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
			for (String customerId : mapUserDateCondition.keySet()) {
				mapCheckDupSMM.put(customerId, new HashSet<>());
			}
			Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
			Map<String, DateTime> mapCheckValidRTP = new HashMap<>();

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();

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
					

					if (mapUserDateCondition.containsKey(customerId) && received_at != null
							&& Utils.SET_APP_NAME_FULL.contains(appName) && Utils.isNumeric(logId)) {
						setLogId.add(logId);

						long duration = new Duration(received_at, mapUserDateCondition.get(customerId))
								.getStandardDays();
						if (duration >= 0 && duration <= 26) {
							boolean willProcessCountLogId = true;

							if (logId.equals("12") || logId.equals("18")) {
								if (sessionMainMenu != null) {
									boolean willProcessSMM = AnalysisUtils.willProcessSessionMainMenu(customerId,
											unparseSMM, sessionMainMenu, received_at, mapCheckDupSMM,
											mapCheckValidSMM);
									if (willProcessSMM) {
										int secondsSMM = (int) new Duration(sessionMainMenu, received_at)
												.getStandardSeconds();
										if (secondsSMM <= 0 || secondsSMM > 12 * 3600) {
											willProcessSMM = false;
										}
									}
									willProcessCountLogId = willProcessSMM;
								}
							} else if (Utils.SET_APP_NAME_RTP.contains(appName)
									&& Utils.SET_LOG_ID_RTP.contains(logId)) {
								boolean willProcessRTP = AnalysisUtils.willProcessRealTimePlaying(customerId,
										received_at, realTimePlaying, mapCheckValidRTP);
								if (willProcessRTP) {
									int seconds = (int) Math.round(realTimePlaying);
									if (seconds > 0 && seconds <= (3 * 3600)) {
										Utils.addMapKeyStrValInt(mapRTPTotalCount, customerId, 1);
										AnalysisUtils.updateAppHourlyDaily(customerId, received_at, appName, seconds,
												totalMapAppRTP, totalMapHourlyRTP, totalMapDailyRTP);
										AnalysisUtils.updateReturnUse(customerId, received_at, seconds,
												mapReturnUsePoint, mapReturnUseCount, mapReturnUseSum,
												mapReturnUseMax);

										countProcess++;
									} else {
										willProcessRTP = false;
									}
								}
								willProcessCountLogId = willProcessRTP;
							}

							if (willProcessCountLogId) {
								Map<String, Integer> mapLogId = totalMapLogId.get(customerId);
								AnalysisUtils.updateCountItem(mapLogId, logId);
								totalMapLogId.put(customerId, mapLogId);
							}
						}
					}

				} else {
					Utils.LOG_ERROR.error("Parsed log error: " + line);
				}

				line = br.readLine();
				count++;
				if (count % 500000 == 0) {
					System.out.println(file.getName() + " | " + count);
				}

			}

			br.close();
			Utils.LOG_INFO.info("Done process total: " + file.getName() + " | Process/Total: " + countProcess + "/"
					+ count + " | Time: " + (System.currentTimeMillis() - start));
		}

		AnalysisUtils.printApp(Utils.getPrintWriter(outputFolderPath + "/vectorApp.csv"), totalMapAppRTP,
				Utils.SET_APP_NAME_RTP);
		AnalysisUtils.printHourly(Utils.getPrintWriter(outputFolderPath + "/vectorHourly.csv"), totalMapHourlyRTP);
		AnalysisUtils.printDaily(Utils.getPrintWriter(outputFolderPath + "/vectorDaily.csv"), totalMapDailyRTP);
		AnalysisUtils.printCountItem(Utils.getPrintWriter(outputFolderPath + "/logIdCount.csv"), setLogId,
				totalMapLogId);
		AnalysisUtils.printReturnUse(Utils.getPrintWriter(outputFolderPath + "/reuseTime.csv"), mapUserDateCondition,
				mapRTPTotalCount, mapReturnUseCount, mapReturnUseSum, mapReturnUseMax);

	}
	
}
