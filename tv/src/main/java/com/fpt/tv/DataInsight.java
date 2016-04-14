package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;

public class DataInsight {
	public static final String DIR = "D:/tv/";
	public static final String CANCEL_CONTRACT_SUMMARY = "D:/tv/HUYBOX.csv";
	public static final String LOG_PATH = "D:/tv/InActiveUser/";
	public static final List<String> LIST_LOG_FILE = Arrays.asList("logt21.txt", "logt22.txt", "logt23.txt",
			"logt24.txt", "logt25.txt", "logt31.txt", "logt32.txt");
	public static final List<String> LIST_TIMESTAMP = Arrays.asList("sp", "0.5", "0.5-1", "1-7", "7-14", ">14");
	public static final Integer[] LOG_START_ID = { 41, 43, 51, 62, 81, 91, 110, 132, 151 };
	public static final Integer[] LOG_STOP_ID = { 42, 44, 52, 63, 82, 133, 152 };

	private Map<String, Integer> mapTimeUseCount = new HashMap<>();
	// private Set<String> setId = new HashSet<>();
	private Map<String, Integer> mapId = new HashMap<>();
	private Map<String, Integer> mapAppIdCount = new HashMap<>();
	private Map<String, Integer> mapLogIdStartCount = new HashMap<>();

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		DataInsight dataInsight = new DataInsight();
		dataInsight.timeUseSummary();
		// dataInsight.logFilter();
		// dataInsight.logIdStartCount();
		// dataInsight.printData();
		// dataInsight.itemTotalCount();
		dataInsight.spendTimeSummary();
		System.out.println("DONE");
	}

	@SuppressWarnings("unused")
	public void spendTimeSummary() throws IOException {
		List<String> listLogIdStop = new ArrayList<>();
		for (Integer i : LOG_STOP_ID) {
			listLogIdStop.add(i.toString());
		}
		
//		PrintWriter prSummary = new PrintWriter(new FileWriter(DIR + "2MonthLogIdStopTimeSummary.csv", true));
//		prSummary.print("," + "dayUse");
//		for (int i = 0; i < listLogIdStop.size(); i++) {
//			prSummary.print("," + listLogIdStop.get(i) + "_Count," + listLogIdStop.get(i) + "_TimeSpend,"
//					+ listLogIdStop.get(i) + "_Average");
//		}
//		prSummary.println();
		
//		PrintWriter prHourly = new PrintWriter(new FileWriter(DIR + "2MonthHourly.csv", true));
//		prHourly.print("," + "dayUse");
//		for (int i = 0; i < 24; i++) {
//			prHourly.print("," + i);
//		}
//		prHourly.println();
		
//		PrintWriter prDaily = new PrintWriter(new FileWriter(DIR + "2MonthDaily.csv", true));
//		prDaily.print("," + "dayUse");
//		for (int i = 1; i <= 7; i++) {
//			prDaily.print("," + i);
//		}
//		prDaily.println();
		
		for (String id : mapId.keySet()) {
			if(!id.equals("526620")){
				continue;
			}else {
				System.out.println(id);
				int dayUse = mapId.get(id);
				Map<String, Integer> mapLogIdStopCount = Collections.synchronizedMap(new HashMap<>());
				Map<String, Double> mapLogIdStopTimeSpend = Collections.synchronizedMap(new HashMap<>());
//			Map<Integer, Integer> mapHourly = Collections.synchronizedMap(new HashMap<>());
//			Map<Integer, Integer> mapDaily = Collections.synchronizedMap(new HashMap<>());
				BufferedReader br = new BufferedReader(new FileReader(DIR + "2MonthLogId.txt"));
				String line = br.readLine();
//				ExecutorService executorService = Executors.newFixedThreadPool(10);
				PrintWriter pr = new PrintWriter(new FileWriter(DIR + "test_zzz.txt"));
				while (line != null) {
					final String finalLine = line;
//					Runnable runnable = new Runnable() {
//						@Override
//						public void run() {
							DataPrepare logRecord = new DataPrepare(finalLine);
							String logId = logRecord.getLogId();
							String recordId = logRecord.getCustomerId();
							if (recordId.equals(id)) {
								if (listLogIdStop.contains(logId)) {
									addCountItemToMap(mapLogIdStopCount, logId);
									Double duration = logRecord.getRealTimePlaying();
									pr.println("LINE: " + finalLine);
									if (mapLogIdStopTimeSpend.containsKey(logId)) {
										pr.println("DURATION: " + duration);
										pr.println("OLD: " + mapLogIdStopTimeSpend.get(logId));
										mapLogIdStopTimeSpend.put(logId, mapLogIdStopTimeSpend.get(logId) + duration);
										pr.println("SUM: " + mapLogIdStopTimeSpend.get(logId));
									} else {
										mapLogIdStopTimeSpend.put(logId, duration);
									}
//								int hour = logRecord.getSessionMainMenu().getHourOfDay();
//								addCountItemToMap(mapHourly, hour);
//								int dayOfWeek = logRecord.getSessionMainMenu().getDayOfWeek();
//								addCountItemToMap(mapDaily, dayOfWeek);
								}
							}
//						}
//					};
//					executorService.execute(runnable);
					line = br.readLine();
				}
//				executorService.shutdown();
//				while (!executorService.isTerminated()) {
//				}
				br.close();
			
//			prSummary.print(id + "," + dayUse);
			for (int i = 0; i < listLogIdStop.size(); i++) {
				String logId = listLogIdStop.get(i);
				if (mapLogIdStopCount.containsKey(logId)) {
					System.out.print("RESULT: "+mapLogIdStopTimeSpend.get(logId) + ",");
//					prSummary.print("," + mapLogIdStopCount.get(logId) + "," + mapLogIdStopTimeSpend.get(logId) + ","
//							+ Math.round(mapLogIdStopTimeSpend.get(logId) / mapLogIdStopCount.get(logId)));
				} else {
//					prSummary.print("," + 0 + "," + 0 + "," + 0);
				}
			}
//			prSummary.println();
			
//			prHourly.print(id + "," + dayUse);
//			for (int i = 0; i < 24; i++) {
//				if (mapHourly.containsKey(i)) {
//					prHourly.print("," + mapHourly.get(i));
//				} else {
//					prHourly.print("," + 0);
//				}
//			}
//			prHourly.println();
			
//			prDaily.print(id + "," + dayUse);
//			for (int i = 1; i <= 7; i++) {
//				if (mapDaily.containsKey(i)) {
//					prDaily.print("," + mapDaily.get(i));
//				} else {
//					prDaily.print("," + 0);
//				}
//			}
//			prDaily.println();
			
			}
		}
		
//		prSummary.close();
//		prHourly.close();
//		prDaily.close();
	}

	public void itemTotalCount() throws IOException {
		List<String> listFile = Arrays.asList("logId0.txt", "logId1.txt", "logId2.txt", "logId3.txt");
		Map<String, Map<String, Integer>> result = new HashMap<>();
		Set<String> setLogId = Collections.synchronizedSet(new HashSet<>());
		System.out.println(listFile);
		for (String file : listFile) {
			ExecutorService executorService = Executors.newFixedThreadPool(10);
			BufferedReader br = new BufferedReader(new FileReader(DIR + file));
			Map<String, Integer> mapCountItem = Collections.synchronizedMap(new HashMap<>());
			String line = br.readLine();
			while (line != null) {
				final String lineFinal = line;
				Runnable runnable = new Runnable() {

					@Override
					public void run() {
						DataPrepare logRecord = new DataPrepare(lineFinal);
						String item = logRecord.getItemName();
						setLogId.add(item);
						addCountItemToMap(mapCountItem, item);
					}
				};
				executorService.execute(runnable);
				line = br.readLine();
			}
			executorService.shutdown();
			while (!executorService.isTerminated()) {
			}
			br.close();
			System.out.println(file);
			String key = file.split("\\.")[0];
			System.out.println(key);
			result.put(key, mapCountItem);
		}
		PrintWriter pr = new PrintWriter(new FileWriter(DIR + "itemNameTotalCount.csv"));
		pr.println(",0,1,2,3");
		for (String logId : setLogId) {
			pr.println(logId + "," + result.get("logId0").get(logId) + "," + result.get("logId1").get(logId) + ","
					+ result.get("logId2").get(logId) + "," + result.get("logId3").get(logId));
		}
		pr.close();
	}

	public void logIdStartCount() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DIR + "logId3.txt"));
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		Set<String> setLogIdStart = new HashSet<>();
		for (int i = 0; i < LOG_START_ID.length; i++) {
			setLogIdStart.add(LOG_START_ID[i].toString());
		}
		String line = br.readLine();
		while (line != null) {
			final String lineFinal = line;
			Runnable runnable = new Runnable() {

				@Override
				public void run() {
					DataPrepare logRecord = new DataPrepare(lineFinal);
					String logId = logRecord.getLogId();
					// System.out.println(logId);
					if (setLogIdStart.contains(logId)) {
						addCountItemToMap(mapLogIdStartCount, logId);
					}
				}
			};
			executorService.execute(runnable);
			line = br.readLine();
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}
		br.close();

	}

	public void logFilter() throws IOException {
		BufferedReader br;
		PrintWriter pr = new PrintWriter(new FileWriter(DIR + "2MonthLogId.txt"));
		AtomicInteger count = new AtomicInteger(0);
		for (String logFile : LIST_LOG_FILE) {
			System.out.println(logFile);
			ExecutorService executorService = Executors.newFixedThreadPool(10);
			br = new BufferedReader(new FileReader(LOG_PATH + logFile));
			String line = br.readLine();
			while (line != null) {
				final String lineFinal = line;
				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						DataPrepare logRecord = new DataPrepare(lineFinal);
						String customerId = logRecord.getCustomerId();
						// if (setId.contains(customerId)) {
						if (mapId.containsKey(customerId)) {
							count.incrementAndGet();
							// String appId = logRecord.getAppId();
							// addCountItemToMap(mapAppIdCount, appId);
							pr.println(lineFinal);
						}

					}
				};
				executorService.execute(runnable);
				line = br.readLine();
			}
			executorService.shutdown();
			while (!executorService.isTerminated()) {
			}
			System.out.println(count);
			br.close();
		}
		pr.close();
	}

	public void timeUseSummary() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(CANCEL_CONTRACT_SUMMARY));
		// PrintWriter pr = new PrintWriter(new FileWriter(DIR +
		// "time_use_summary.csv"));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			CancelRecord record = new CancelRecord(line);
			if (record.getDayUse() > 4 && record.getDayUse() <= 60) {
				// setId.add(record.getId());
				mapId.put(record.getId(), (int) record.getDayUse());
			}
			// countByDayUse(mapTimeUseCount, record.getDayUse());
			addCountItemToMap(mapTimeUseCount, String.valueOf(record.getDayUse()));
			line = br.readLine();
		}
		br.close();
	}

	public void printData() throws IOException {
		PrintWriter pr;
		String pathTimeUseCount = DIR + "timeUseCount.csv";
		String pathSetId = DIR + "2MonthSetId.txt";
		String pathAppIdCount = DIR + "appCountId0.csv";
		String pathLogIdStartCount = DIR + "logIdStart3.csv";

		// pr = new PrintWriter(new FileWriter(pathTimeUseCount));
		// for (String key : mapTimeUseCount.keySet()) {
		// pr.println(key + "," + mapTimeUseCount.get(key));
		// }
		// pr.close();

		pr = new PrintWriter(new FileWriter(pathSetId));
		for (String id : mapId.keySet()) {
			pr.println(id + "," + mapId.get(id));
		}
		pr.close();

		// pr = new PrintWriter(new FileWriter(pathAppCountId1));
		// for (String key : mapAppIdCount.keySet()) {
		// pr.println(key + "," + mapAppIdCount.get(key));
		// }
		// pr.close();

		// pr = new PrintWriter(new FileWriter(pathLogIdStartCount));
		// for (String key : mapLogIdStartCount.keySet()) {
		// pr.println(key + "," + mapLogIdStartCount.get(key));
		// }
		// pr.close();
	}

	public static void countByDayUse(Map<String, Integer> mapTest, long dayUse) {
		String name = "";
		if (dayUse <= 1) {
			name = LIST_TIMESTAMP.get(0);
		} else if (dayUse > 1 && dayUse <= 15) {
			name = LIST_TIMESTAMP.get(1);
		} else if (dayUse > 15 && dayUse <= 30) {
			name = LIST_TIMESTAMP.get(2);
		} else if (dayUse > 30 && dayUse <= 210) {
			name = LIST_TIMESTAMP.get(3);
		} else if (dayUse > 210 && dayUse <= 420) {
			name = LIST_TIMESTAMP.get(4);
		} else if (dayUse > 420) {
			name = LIST_TIMESTAMP.get(5);
		}
		addCountItemToMap(mapTest, name);
	}

	public static void addCountItemToMap(Map<String, Integer> mapCount, String item) {
		if (mapCount.containsKey(item)) {
			mapCount.put(item, mapCount.get(item) + 1);
		} else {
			mapCount.put(item, 1);
		}
	}

	public static void addCountItemToMap(Map<Integer, Integer> mapCount, Integer item) {
		if (mapCount.containsKey(item)) {
			mapCount.put(item, mapCount.get(item) + 1);
		} else {
			mapCount.put(item, 1);
		}
	}
}
