package com.fpt.tv.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

import com.fpt.tv.utils.AnalysisUtils;
import com.fpt.tv.utils.CommonConfig;
import com.fpt.tv.utils.SupportData;
import com.fpt.tv.utils.Utils;

public class ActionAnalysis {
	private Map<String, DateTime> mapUserDateCondition;
	private List<String> listFileLogPath;
	private CommonConfig cf; 

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		Utils.LOG_INFO.info("-----------------------------------------------------");
		long star = System.currentTimeMillis();

		ActionAnalysis actionAnalysis = new ActionAnalysis();
		actionAnalysis.getLogIdCount();

		long end = System.currentTimeMillis();
		System.out.println("DONE: " + (end - star));
	}

	public ActionAnalysis() throws IOException {
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

	public void getLogIdCount() throws IOException {
		Map<String, Map<String, Integer>> totalMapLogId = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : mapUserDateCondition.keySet()) {
			totalMapLogId.put(customerId, new HashMap<>());
		}

		Set<String> setLogId = new HashSet<>();
		ExecutorService executorService = Executors.newFixedThreadPool(3);
		for (String file : listFileLogPath) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int count = 0;
					int countProcess = 0;
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
							Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
							String unparseSMM = arr[6];
							DateTime sessionMainMenu = Utils.parseSessionMainMenu(arr[6]);
							DateTime received_at = Utils.parseReceived_at(arr[8]);
							if (mapUserDateCondition.containsKey(customerId) && sessionMainMenu != null
									&& received_at != null && Utils.LIST_APP_NAME.contains(appName)) {
								long duration = new Duration(sessionMainMenu, mapUserDateCondition.get(customerId)).getStandardDays();
								if (duration >= 0 && duration <= 27) {
									boolean willProcessSMM = TimeUseAnalysis.willProcessSessionMainMenu(customerId,
											logId, unparseSMM, sessionMainMenu, received_at, mapCheckDupSMM,
											mapCheckValidSMM);
									boolean willProcessRTP = TimeUseAnalysis.willProcessRealTimePlaying(customerId,
											received_at, realTimePlaying, mapCheckValidRTP);
									boolean willProcess = willProcessActionCount(logId, realTimePlaying,
											sessionMainMenu, received_at, willProcessSMM, willProcessRTP);

									if (willProcess) {
										setLogId.add(logId);
										Map<String, Integer> mapLogId = totalMapLogId.get(customerId);
										AnalysisUtils.updateCountItem(mapLogId, logId);
										totalMapLogId.put(customerId, mapLogId);
										countProcess++;
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

					Utils.LOG_INFO.info(
							"Done process file: " + file.split("/")[file.split("/").length - 1] + " | Valid/Total: "
									+ countProcess + "/" + count + " | Time: " + (System.currentTimeMillis() - start));
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		AnalysisUtils.printCountItem(Utils.getPrintWriter(cf.get(CommonConfig.MAIN_DIR) + "/countLogId.csv"), setLogId, totalMapLogId);

	}

	private boolean willProcessActionCount(String logId, Double realTimePlaying, DateTime sessionMainMenu,
			DateTime received_at, boolean willProcessSMM, boolean willProcessRTP) {
		boolean willProcess = false;

		if (willProcessSMM) {
			int secondsSMM = (int) new Duration(sessionMainMenu, received_at).getStandardSeconds();
			if (secondsSMM > 0 && secondsSMM <= 12 * 3600) {
				willProcess = true;
			}
		} else if (willProcessRTP) {
			int secondsRTP = (int) Math.round(realTimePlaying);
			if (secondsRTP > 0 && secondsRTP <= 3 * 3600) {
				willProcess = true;
			}
		} else if (Utils.isNumeric(logId)) {
			willProcess = true;
		}
		return willProcess;
	}
}
