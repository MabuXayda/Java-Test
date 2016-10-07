package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class UserUsage {
	private List<String> listExtra = new ArrayList<>();
	private Map<String, String> mapUserContract;
	private Map<String, Map<String, Integer>> mapUserVectorApp;
	private Map<String, Map<Integer, Integer>> mapUserVectorHourly;
	private static UserUsage instance;
	private static String status;

	private UserUsage() {

	}

	public static UserUsage getInstance() {
		if (instance == null) {
			synchronized (UserUsage.class) {
				if (instance == null) {
					instance = new UserUsage();
				}
			}
		}
		return instance;
	}

	public void dumpCheck() throws IOException {
		Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader("/home/tunn/data/tv/617915.csv"));
		PrintWriter pr = new PrintWriter(new FileWriter("/home/tunn/data/tv/617915_flag.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			String customerId = arr[0];
			String logId = arr[2];
			String appName = arr[3];
			Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
			String unparseSMM = arr[6];
			DateTime sessionMainMenu = PayTVUtils.parseSessionMainMenu(arr[6]);
			DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);

			int secondsSMM = 0;
			int secondsRTP = 0;
			// boolean willProcessCountLogId = false;
			boolean willProcessSMM = false;
			boolean willProcessRTP = false;

			if (StringUtils.isNumeric(customerId) && received_at != null
					&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
				// willProcessCountLogId = true;
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
					// willProcessCountLogId = willProcessSMM;
				} else if (PayTVUtils.SET_APP_NAME_RTP.contains(appName) && PayTVUtils.SET_LOG_ID_RTP.contains(logId)) {
					willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId, received_at, realTimePlaying,
							mapCheckValidRTP);
					System.out.println(willProcessRTP + " | " + line);
					if (willProcessRTP) {
						secondsRTP = (int) Math.round(realTimePlaying);
						if (secondsRTP <= 0 || secondsRTP > 3 * 3600) {
							willProcessRTP = false;
						}
					}
					// willProcessCountLogId = willProcessRTP;
				}
			}
			pr.println(customerId + "," + logId + "," + appName + "," + arr[4] + "," + realTimePlaying + ","
					+ PayTVUtils.FORMAT_DATE_TIME.print(sessionMainMenu) + ","
					+ PayTVUtils.FORMAT_DATE_TIME.print(received_at) + "," + willProcessRTP);
			line = br.readLine();
		}
		br.close();
		pr.close();
	}

	public void statisticUserUsageCheck(String folderPath, String outputPath) throws IOException {
		HdfsIO hdfsIO = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE),
				CommonConfig.get(PayTVConfig.HDFS_SITE));
		List<String> listFile = hdfsIO.getListFileInDir(folderPath);
		initMap();
		for (String file : listFile) {
			statisticUserUsage(file, hdfsIO);
		}
		PrintWriter pr = new PrintWriter(new File(outputPath + "/vectorHourly.csv"));
		StatisticUtils.printHourly(pr, mapUserVectorHourly);
		pr = new PrintWriter(new File(outputPath + "/vectorApp.csv"));
		StatisticUtils.printApp(pr, mapUserVectorApp, PayTVUtils.SET_APP_NAME_RTP);
	}

	public void initMap() {
		mapUserContract = new ConcurrentHashMap<>();
		mapUserVectorHourly = new ConcurrentHashMap<>();
		mapUserVectorApp = new ConcurrentHashMap<>();
	}

	public void initExtra() {
		listExtra = new ArrayList<>();
	}

	public void statisticUserUsageExtra() {
		System.out.println("List extra log size:" + listExtra.size());
		long start = System.currentTimeMillis();
		int countTime = 0;
		Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
		for (String line : listExtra) {
			String[] arr = line.split(",");
			String customerId = arr[0];
			String contract = arr[1];
			String logId = arr[2];
			String appName = arr[3];
			Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[4]);
			String unparseSMM = arr[5];
			DateTime sessionMainMenu = PayTVUtils.parseSessionMainMenu(arr[5]);
			DateTime received_at = PayTVUtils.parseReceived_at(arr[6]);
			int secondsSMM = 0;
			int secondsRTP = 0;
			// boolean willProcessCountLogId = false;
			boolean willProcessSMM = false;
			boolean willProcessRTP = false;
			if (StringUtils.isNumeric(customerId) && received_at != null
					&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
				// willProcessCountLogId = true;
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
					// willProcessCountLogId = willProcessSMM;
				} else if (PayTVUtils.SET_APP_NAME_RTP.contains(appName) && PayTVUtils.SET_LOG_ID_RTP.contains(logId)) {
					willProcessRTP = StatisticUtils.willProcessRealTimePlaying(customerId, received_at, realTimePlaying,
							mapCheckValidRTP);
					if (willProcessRTP) {
						secondsRTP = (int) Math.round(realTimePlaying);
						if (secondsRTP <= 0 || secondsRTP > 3 * 3600) {
							willProcessRTP = false;
						}
					}
					// willProcessCountLogId = willProcessRTP;
				}
			}
			if (willProcessRTP) {
				if (!mapUserContract.containsKey(customerId)) {
					mapUserContract.put(customerId, contract);
					mapUserVectorHourly.put(customerId, new ConcurrentHashMap<>());
					mapUserVectorApp.put(customerId, new ConcurrentHashMap<>());
				}
				countTime++;
				Map<Integer, Integer> mapHourly = mapUserVectorHourly.get(customerId);
				StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
				mapUserVectorHourly.put(customerId, mapHourly);

				Map<String, Integer> mapApp = mapUserVectorApp.get(customerId);
				StatisticUtils.updateApp(mapApp, appName, secondsRTP);
				mapUserVectorApp.put(customerId, mapApp);
			}
		}
		status = ">>> Done calculate extra | validTime: " + countTime + " | Time: "
				+ (System.currentTimeMillis() - start);
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

	public void statisticUserUsage(String filePath, HdfsIO hdfsIO) throws IOException {
		long start = System.currentTimeMillis();
		int countTotal = 0;
		int countTime = 0;
		Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
		BufferedReader br = hdfsIO.getReadStream(filePath);
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length >= 9) {
				String customerId = arr[0];
				String contract = arr[1];
				String logId = arr[2];
				String appName = arr[3];
				Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
				String unparseSMM = arr[6];
				DateTime sessionMainMenu = PayTVUtils.parseSessionMainMenu(arr[6]);
				DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);
				int secondsSMM = 0;
				int secondsRTP = 0;
				// boolean willProcessCountLogId = false;
				boolean willProcessSMM = false;
				boolean willProcessRTP = false;
				// ===================== START split extra
				DateTime dateTimeAtStartOfDate = received_at.withTimeAtStartOfDay();
				Double validTime = (double) new Duration(dateTimeAtStartOfDate, received_at).getStandardSeconds();
				if (realTimePlaying != null && validTime < realTimePlaying) {
					if (realTimePlaying <= 3 && realTimePlaying > 0) {
						DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
						Double newRealTimePlaying = realTimePlaying - validTime;
						String lineDump = customerId + "," + contract + "," + logId + "," + appName + ","
								+ newRealTimePlaying + "," + unparseSMM + "," + dtf.print(dateTimeAtStartOfDate)
								+ "+07:00";
						listExtra.add(lineDump);
						realTimePlaying = validTime;
					}
				}
				// ===================== END split extra
				if (StringUtils.isNumeric(customerId) && received_at != null
						&& PayTVUtils.SET_APP_NAME_FULL.contains(appName) && StringUtils.isNumeric(logId)) {
					// willProcessCountLogId = true;
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
						// willProcessCountLogId = willProcessSMM;
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
						// willProcessCountLogId = willProcessRTP;
					}
				}
				if (willProcessRTP) {
					if (!mapUserContract.containsKey(customerId)) {
						mapUserContract.put(customerId, contract);
						mapUserVectorHourly.put(customerId, new ConcurrentHashMap<>());
						mapUserVectorApp.put(customerId, new ConcurrentHashMap<>());
					}
					countTime++;
					Map<Integer, Integer> mapHourly = mapUserVectorHourly.get(customerId);
					StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
					mapUserVectorHourly.put(customerId, mapHourly);

					Map<String, Integer> mapApp = mapUserVectorApp.get(customerId);
					StatisticUtils.updateApp(mapApp, appName, secondsRTP);
					mapUserVectorApp.put(customerId, mapApp);
				}
			} else {
				PayTVUtils.LOG_ERROR.error("LOG PARSE ERROR: " + line);
			}
			countTotal++;
			line = br.readLine();
		}
		br.close();
		status = "======= Done calculate user usage: " + filePath.split("/")[filePath.split("/").length - 1]
				+ " | Total: " + countTotal + " | validTime: " + countTime + " | Time: "
				+ (System.currentTimeMillis() - start);
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

	public Map<String, String> getMapUserContract() {
		return mapUserContract;
	}

	public Map<String, Map<String, Integer>> getMapUserVectorApp() {
		return mapUserVectorApp;
	}

	public Map<String, Map<Integer, Integer>> getMapUserVectorHourly() {
		return mapUserVectorHourly;
	}
}
