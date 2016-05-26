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

import com.fpt.tv.utils.ActionUtils;
import com.fpt.tv.utils.SupportDataUtils;
import com.fpt.tv.utils.Utils;

public class ActionAnalysis {
	private Map<String, DateTime> sp_mapUserDateCondition;
	private List<String> listFileLogPath;

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

	public void getLogIdCount() throws IOException {
		Map<String, Map<String, Integer>> totalMapLogId = Collections.synchronizedMap(new HashMap<>());
		for (String customerId : sp_mapUserDateCondition.keySet()) {
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
					for (String customerId : sp_mapUserDateCondition.keySet()) {
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
							boolean willProcessSMM = TimeUseAnalysis.willProcessSessionMainMenu(customerId, appName,
									logId, unparseSMM, sessionMainMenu, received_at, mapCheckDupSMM, mapCheckValidSMM,
									sp_mapUserDateCondition);
							boolean willProcessRTP = TimeUseAnalysis.willProcessRealTimePlaying(customerId, appName,
									sessionMainMenu, received_at, realTimePlaying, mapCheckValidRTP, mapCheckValidSMM);
							boolean willProcess = willProcessActionCount(customerId, logId, realTimePlaying,
									sessionMainMenu, received_at, willProcessSMM, willProcessRTP,
									sp_mapUserDateCondition);

							if (willProcess) {
								setLogId.add(logId);
								Map<String, Integer> mapLogId = totalMapLogId.get(customerId);
								ActionUtils.updateCountItem(mapLogId, logId);
								totalMapLogId.put(customerId, mapLogId);
								countProcess++;
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

		ActionUtils.printCountItem(Utils.getPrintWriter(Utils.DIR + "countLogId.csv"), setLogId, totalMapLogId);

	}

	private boolean willProcessActionCount(String customerId, String logId, Double realTimePlaying,
			DateTime sessionMainMenu, DateTime received_at, boolean willProcessSMM, boolean willProcessRTP,
			Map<String, DateTime> mapUserDateCondition) {
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
		} else if (Utils.isNumeric(logId) && mapUserDateCondition.containsKey(customerId)) {
			if (new Duration(sessionMainMenu, mapUserDateCondition.get(customerId)).getStandardDays() <= 28) {
				willProcess = true;
			}
		}
		return willProcess;
	}
}
