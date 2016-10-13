package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.StatisticUtils;

public class UserUsageDetailApp {
	private Map<String, Map<String, Map<Integer, Integer>>> mapUserAppDetailHourly;
	private Map<String, Map<String, Map<String, Integer>>> mapUserAppDetailDaily;
	private static UserUsageDetailApp instance;
	private static String status;
	private static Set<String> setUser;

	private UserUsageDetailApp() {
	}

	public static UserUsageDetailApp getInstance() {
		if (instance == null) {
			synchronized (UserUsageDetailApp.class) {
				if (instance == null) {
					instance = new UserUsageDetailApp();
				}
			}
		}
		return instance;
	}

	public static void main(String[] args) throws IOException {
		UserUsageDetailApp userUsageDetail = new UserUsageDetailApp();
		userUsageDetail.process("/data/payTV/log_parsed/2016/09/18/");

	}

	public void process(String folderPath) throws IOException {
		HdfsIO hdfsIO = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE),
				CommonConfig.get(PayTVConfig.HDFS_SITE));
		List<String> listFile = hdfsIO.getListFileInDir(folderPath);
		initMap();
		for (String file : listFile) {
			statisticUserUsageDetail(file, hdfsIO);
		}

		PrintWriter pr = new PrintWriter(
				new FileWriter(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/userUsageDetailApp.csv"));
		StatisticUtils.printDetailApp(pr, mapUserAppDetailHourly);
	}

	public void initMap() {
		setUser = ConcurrentHashMap.newKeySet();
		mapUserAppDetailHourly = new ConcurrentHashMap<>();
		mapUserAppDetailDaily = new ConcurrentHashMap<>();
	}

	public void statisticUserUsageDetail(String filePath, HdfsIO hdfsIO) throws IOException {
		long start = System.currentTimeMillis();
		int countTotal = 0;
		int countTime = 0;
		Map<String, Set<String>> mapCheckDupSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidSMM = new HashMap<>();
		Map<String, DateTime> mapCheckValidRTP = new HashMap<>();
		BufferedReader br = hdfsIO.getReadStream(filePath);
		String line = br.readLine();
		while (line != null) {
			countTotal++;
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
							if (secondsSMM <= 0 || secondsSMM > PayTVConfig.getSMMMax()) {
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
						if (secondsRTP <= 0 || secondsRTP > PayTVConfig.getRTPMax()) {
							willProcessRTP = false;
						}
					}
					// willProcessCountLogId = willProcessRTP;
				}
			}
			if (willProcessRTP) {
				if (!setUser.contains(customerId)) {
					setUser.add(customerId);
					mapUserAppDetailHourly.put(customerId, new ConcurrentHashMap<>());
					mapUserAppDetailDaily.put(customerId, new ConcurrentHashMap<>());
				}
				if (mapUserAppDetailHourly.get(customerId).get(appName) == null) {
					Map<String, Map<Integer, Integer>> temp = mapUserAppDetailHourly.get(customerId);
					temp.put(appName, new ConcurrentHashMap<>());
					mapUserAppDetailHourly.put(customerId, temp);
				}
				if (mapUserAppDetailDaily.get(customerId).get(appName) == null) {
					Map<String, Map<String, Integer>> temp = mapUserAppDetailDaily.get(customerId);
					temp.put(appName, new ConcurrentHashMap<>());
					mapUserAppDetailDaily.put(customerId, temp);
				}
				countTime++;
				Map<String, Map<Integer, Integer>> mapDetailHourly = mapUserAppDetailHourly.get(customerId);
				Map<Integer, Integer> mapHourly = mapDetailHourly.get(appName);
				StatisticUtils.updateHourly(mapHourly, received_at, secondsRTP);
				mapDetailHourly.put(appName, mapHourly);
				mapUserAppDetailHourly.put(customerId, mapDetailHourly);

				Map<String, Map<String, Integer>> mapDetailDaily = mapUserAppDetailDaily.get(customerId);
				Map<String, Integer> mapDaily = mapDetailDaily.get(appName);
				StatisticUtils.updateDaily(mapDaily, received_at, secondsRTP);
				mapDetailDaily.put(appName, mapDaily);
				mapUserAppDetailDaily.put(customerId, mapDetailDaily);
			}
			line = br.readLine();
		}
		br.close();
		status = "Done calculate user usage: " + filePath.split("/")[filePath.split("/").length - 1] + " | Total: "
				+ countTotal + " | validTime: " + countTime + " | Time: " + (System.currentTimeMillis() - start);
		PayTVUtils.LOG_INFO.info(status);
		System.out.println(status);
	}

	public Map<String, Map<String, Map<Integer, Integer>>> getMapUserAppDetailHourly() {
		return mapUserAppDetailHourly;
	}

	public Map<String, Map<String, Map<String, Integer>>> getMapUserAppDetailDaily() {
		return mapUserAppDetailDaily;
	}

	public Set<String> getSetUser() {
		return setUser;
	}
}
