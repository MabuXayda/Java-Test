package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;

public class DataPrepare {
	public static final String DIR = "/home/tunn/data/tv/";

	public static void test() {
		String str = "2016:09:19:02:01:30";
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		DateTime time = dtf.parseDateTime(str);
		System.out.println(time.getHourOfDay());
		int duration = time.getMinuteOfHour() * 60 + time.getSecondOfMinute();
		System.out.println(duration);
		time = time.minusSeconds(duration);
		System.out.println(time.getHourOfDay());
		time = time.minusSeconds(3600);
		System.out.println(time.getHourOfDay());
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		System.out.println("START");
		long star = System.currentTimeMillis();
		DataPrepare logRecord = new DataPrepare();
		logRecord.filterSessionMainMenu();
		long end = System.currentTimeMillis();
		System.out.println("DONE: " + (end - star));
	}

	public void filterSessionMainMenu() throws IOException, InterruptedException {
		File[] files = new File(DIR + "parsed_sample/").listFiles();
		Map<String, Map<String, Integer>> totalMapApp = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<Integer, Integer>> totalMapHourly = Collections.synchronizedMap(new HashMap<>());
		Map<String, Map<String, Integer>> totalMapDaily = Collections.synchronizedMap(new HashMap<>());
		AtomicInteger count = new AtomicInteger(0);
		for (File file : files) {
			System.out.println(file.getAbsolutePath());
			ExecutorService executorService = Executors.newFixedThreadPool(10);
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				String record = line;
//				System.out.println(record);
				// executorService.execute(new Runnable() {
				// @Override
				// public void run() {
				LogObject logObject = new LogObject(record);
				if (logObject.getLogId().equals("12") || logObject.getLogId().equals("18")) {
					if (logObject.getCustomerId() != null && !logObject.getCustomerId().isEmpty()) {
						if (logObject.getAppName() != null && !logObject.getAppName().isEmpty()) {
							if (logObject.getSessionMainMenu() != null && logObject.getReceived_at() != null) {
								Duration duration = new Duration(logObject.getSessionMainMenu(),
										logObject.getReceived_at());
								int seconds = (int) duration.getStandardSeconds();
								Map<Integer, Integer> mapHourly = new HashMap<>();
								if (totalMapHourly.containsKey(logObject.getCustomerId())) {
									mapHourly = totalMapHourly.get(logObject.getCustomerId());
								}
								Map<String, Integer> mapDaily = new HashMap<>();
								if (totalMapDaily.containsKey(logObject.getCustomerId())) {
									mapDaily = totalMapDaily.get(logObject.getCustomerId());
								}
								updateHourlyDaily(mapHourly, mapDaily, logObject.getReceived_at(), seconds);
								totalMapHourly.put(logObject.getCustomerId(), mapHourly);
								totalMapDaily.put(logObject.getCustomerId(), mapDaily);
							}
						}
					}
				}
				count.incrementAndGet();
				if (count.get() % 100000 == 0) {
					System.out.println(count);
					count.set(0);
				}
				// }
				// });
				line = br.readLine();
			}
			// executorService.shutdown();
			// while (!executorService.isTerminated()) {
			// }
			br.close();
		}
		PrintWriter prHourly = new PrintWriter(new FileWriter(DIR + "sample_hourly.csv"));
		PrintWriter prDaily = new PrintWriter(new FileWriter(DIR + "sample_daily.csv"));
		prHourly.print("CustomerId");
		for (int i = 0; i < 24; i++) {
			prHourly.print("," + i);
		}
		prHourly.println();
		List<String> listDay = Arrays.asList("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun");
		prDaily.print("CustomerId");
		for (String day : listDay) {
			prDaily.print("," + day);
		}
		prDaily.println();
		for (String customerId : totalMapHourly.keySet()) {
			Map<Integer, Integer> mapHourly = totalMapHourly.get(customerId);
			Map<String, Integer> mapDaily = totalMapDaily.get(customerId);
			prHourly.print(customerId);
			for (int i = 0; i < 24; i++) {
				prHourly.print("," + mapHourly.get(i));
			}
			prHourly.println();
			prDaily.print(customerId);
			for (String day : listDay) {
				prDaily.print("," + mapDaily.get(day));
			}
			prDaily.println();
		}
		prHourly.close();
		prDaily.close();
	}

	public void filterRawLogs() throws IOException {
		File[] files = new File(DIR + "log/").listFiles();
		File theDir = new File(DIR + "log_parsed");
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}
		AtomicInteger count = new AtomicInteger(0);
		for (File file : files) {
			long start = System.currentTimeMillis();
			// tao file ghi

			String output = DIR + "log_parsed/" + file.getName().split("\\.")[0] + "_parsed.csv";
			PrintWriter pr = new PrintWriter(new FileWriter(output));
			pr.println("CustomerId,Contract,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			// tao threadpool
			ExecutorService executorService = Executors.newFixedThreadPool(20);

			// bat dau doc file
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				// System.out.println(line);
				String gsonLine = line;
				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						if (gsonLine != null && !gsonLine.isEmpty()) {
							Source source = new Gson().fromJson(gsonLine, Source.class);
							String customerId = source.getFields().getCustomerId();
							String contract = source.getFields().getContract();
							String logId = source.getFields().getLogId();
							String appName = source.getFields().getAppName();
							String itemId = source.getFields().getItemId();
							String realTimePlaying = source.getFields().getRealTimePlaying();
							String sessionMainMenu = source.getFields().getSessionMainMenu();
							String boxTime = source.getFields().getBoxTime();
							String received_at = source.getReceived_at();
							pr.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId + ","
									+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at);
							count.incrementAndGet();
							if (count.get() % 100000 == 0) {
								System.out.println(count);

							}
						}
					}
				};
				executorService.execute(runnable);
				line = br.readLine();
			}
			executorService.shutdown();
			while (!executorService.isTerminated()) {
			}
			long end = System.currentTimeMillis();
			System.out.println("===> Done file: " + file.getName());
			System.out.println("===> Count: " + count + " - Time: " + (end - start));
			count.set(0);
			br.close();
			pr.close();
		}
	}

	public void filterRawLogs2() throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(DIR + "parsed.csv"));
		PrintWriter prTest = new PrintWriter(new FileWriter(DIR + "monthError.csv"));
		pr.println("CustomerId,AppId,LogId,Event,ItemId,RealTimePlaying,Hour,DayOfWeek,Date");
		File[] files = new File(DIR + "raw/").listFiles();
		AtomicInteger count = new AtomicInteger(0);
		Map<String, String> mapTemp = Collections.synchronizedMap(new HashMap<>());
		for (File file : files) {
			System.out.println(file.getAbsolutePath());
			BufferedReader br = new BufferedReader(new FileReader(file));
			ExecutorService executorService = Executors.newFixedThreadPool(10);
			String line = br.readLine();
			while (line != null) {

				String gsonLine;
				if (!String.valueOf(line.charAt(0)).equals("{")) {
					gsonLine = line.substring(1);
				} else {
					gsonLine = line;
				}
				Runnable runnable = new Runnable() {

					@SuppressWarnings("unused")
					@Override
					public void run() {
						if (gsonLine != null && !gsonLine.isEmpty()) {
							Record record = new Gson().fromJson(gsonLine, Record.class);
							String customerId = record.getSource().getFields().getCustomerId();
							String logId = record.getSource().getFields().getLogId();
							String itemId = record.getSource().getFields().getItemId();
							String sessionMainMenu = record.getSource().getFields().getSessionMainMenu();
							Integer hour = null;
							String dayOfWeek = null;
							String day = null;
							Double realTimePlaying = record.getSource().getFields().getRealTimePlayingValue();
							DateTime date = null;
							// if (sessionMainMenu != null) {
							// date =
							// LogUtils.getDateFromSession(sessionMainMenu);
							// DateTimeFormatter dtfOut =
							// DateTimeFormat.forPattern("dd/MM/yyyy");
							// hour = date.getHourOfDay();
							// dayOfWeek = LogUtils.getDayOfWeek(date);
							// day = dtfOut.print(date);
							// int month = date.getMonthOfYear();
							// if(month != 3 && month !=2)
							// {
							// prTest.println(gsonLine);
							// count.incrementAndGet();
							// if (count.get() % 100 == 0) {
							// System.out.println(count);
							// }
							// }
							// }

							if (realTimePlaying == null || date == null) {
								pr.println(customerId + "," + logId + "," + itemId + "," + realTimePlaying + "," + hour
										+ "," + dayOfWeek + "," + day);
							} else if (date != null && realTimePlaying != null) {
								int minute = date.getMinuteOfHour();
								int second = date.getSecondOfMinute();
								double currentTime = minute * 60 + second;
								pr.println(customerId + "," + logId + "," + itemId + ","
										+ Math.min(currentTime, realTimePlaying) + "," + hour + "," + dayOfWeek + ","
										+ day);

								double beforeTime = realTimePlaying - currentTime;
								if (beforeTime > 0) {
									date = date.minusSeconds((int) currentTime);
									int times = (int) (beforeTime / 3600);
									double timeFinal = beforeTime % 3600;
									for (int i = 1; i <= times; i++) {
										date = date.minusSeconds(3600);
										hour = date.getHourOfDay();
										dayOfWeek = LogUtils.getDayOfWeek(date);
										pr.println(customerId + "," + logId + "," + itemId + "," + 3600 + "," + hour
												+ "," + dayOfWeek + "," + day);
									}
									date = date.minusSeconds((int) timeFinal);
									hour = date.getHourOfDay();
									dayOfWeek = LogUtils.getDayOfWeek(date);
									pr.println(customerId + "," + logId + "," + itemId + "," + timeFinal + "," + hour
											+ "," + dayOfWeek + "," + day);
								}
							}

							// count.incrementAndGet();
							// if (count.get() % 10000 == 0) {
							// System.out.println(count);
							// }
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
		for (String key : mapTemp.keySet()) {
			prTest.println(key + "," + mapTemp.get(key));
		}
		pr.close();
		prTest.close();
	}

	private void updateHourlyDaily(Map<Integer, Integer> mapHourly, Map<String, Integer> mapDaily, DateTime stopTime,
			int duration) {

		int currentTime = stopTime.getMinuteOfHour() * 60 + stopTime.getSecondOfMinute();
		int currentHour = stopTime.getHourOfDay();
		String currentDay = LogUtils.getDayOfWeek(stopTime);
		addMapIntInt(mapHourly, currentHour, currentTime);
		addMapStringInt(mapDaily, currentDay, currentTime);

		int beforeTime = duration - currentTime;
		if (beforeTime > 0) {
			stopTime = stopTime.minusSeconds(currentTime);
			int times = beforeTime / 3600;
			int finalTime = beforeTime % 3600;
			for (int i = 1; i <= times; i++) {
				stopTime = stopTime.minusSeconds(3600);
				currentHour = stopTime.getHourOfDay();
				currentDay = LogUtils.getDayOfWeek(stopTime);
				addMapIntInt(mapHourly, currentHour, 3600);
				addMapStringInt(mapDaily, currentDay, 3600);
			}
			stopTime = stopTime.minus(finalTime);
			currentHour = stopTime.getHourOfDay();
			currentDay = LogUtils.getDayOfWeek(stopTime);
			addMapIntInt(mapHourly, currentHour, finalTime);
			addMapStringInt(mapDaily, currentDay, finalTime);
		}
	}

	private void addMapIntInt(Map<Integer, Integer> mapMain, Integer key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}

	private void addMapStringInt(Map<String, Integer> mapMain, String key, Integer value) {
		if (mapMain.containsKey(key)) {
			mapMain.put(key, mapMain.get(key) + value);
		} else {
			mapMain.put(key, value);
		}
	}
}
