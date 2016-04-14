package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;

public class DataPrepare {
	public static final String DIR = "/home/tunn/data/tv/";

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		DataPrepare logRecord = new DataPrepare();
		logRecord.filterRawLogs();
		System.out.println("DONE");
	}

	public void filterRawLogs() throws IOException {
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

					@Override
					public void run() {
						if (gsonLine != null && !gsonLine.isEmpty()) {
							Record record = new Gson().fromJson(gsonLine, Record.class);
							String customerId = record.getSource().getFields().getCustomerId();
							String appId = record.getSource().getFields().getAppId();
							String logId = record.getSource().getFields().getLogId();
							String event = record.getSource().getFields().getEvent();
							String itemId = record.getSource().getFields().getItemId();
							String sessionMainMenu = record.getSource().getFields().getSessionMainMenu();
							Integer hour = null;
							String dayOfWeek = null;
							String day = null;
							Double realTimePlaying = record.getSource().getFields().getRealTimePlayingValue();
							DateTime date = null;
							if (logId!=null && !logId.isEmpty() && event!=null && !event.isEmpty()) {
								mapTemp.put(logId, event);
							}
//							if (sessionMainMenu != null) {
//								date = LogUtils.getDateFromSession(sessionMainMenu);
//								DateTimeFormatter dtfOut = DateTimeFormat.forPattern("dd/MM/yyyy");
//								hour = date.getHourOfDay();
//								dayOfWeek = LogUtils.getDayOfWeek(date);
//								day = dtfOut.print(date);
//								int month = date.getMonthOfYear();
//								if(month != 3 && month !=2)
//								{
//									prTest.println(gsonLine);
//									count.incrementAndGet();
//									if (count.get() % 100 == 0) {
//										System.out.println(count);
//									}
//								}
//							}

							// if (realTimePlaying == null || date == null) {
							// pr.println(customerId + "," + appId + "," + logId
							// + "," + event + "," + itemId + ","
							// + realTimePlaying + "," + hour + "," + dayOfWeek
							// + "," + day);
							// } else if (date != null && realTimePlaying !=
							// null) {
							// int minute = date.getMinuteOfHour();
							// int second = date.getSecondOfMinute();
							// double currentTime = minute * 60 + second;
							// pr.println(customerId + "," + appId + "," + logId
							// + "," + event + "," + itemId + ","
							// + Math.min(currentTime, realTimePlaying) + "," +
							// hour + "," + dayOfWeek + ","
							// + day);
							//
							// double beforeTime = realTimePlaying -
							// currentTime;
							// if (beforeTime > 0) {
							// date=date.minusSeconds((int) currentTime);
							// int times = (int) (beforeTime / 3600);
							// double timeFinal = beforeTime % 3600;
							// for (int i = 1; i <= times; i++) {
							// date = date.minusSeconds(3600);
							// hour = date.getHourOfDay();
							// dayOfWeek = LogUtils.getDayOfWeek(date);
							// pr.println(customerId + "," + appId + "," + logId
							// + "," + event + "," + itemId
							// + "," + 3600 + "," + hour + "," + dayOfWeek + ","
							// + day);
							// }
							// date = date.minusSeconds((int) timeFinal);
							// hour = date.getHourOfDay();
							// dayOfWeek = LogUtils.getDayOfWeek(date);
							// pr.println(customerId + "," + appId + "," + logId
							// + "," + event + "," + itemId + ","
							// + timeFinal + "," + hour + "," + dayOfWeek + ","
							// + day);
							// }
							// }

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
		for(String key : mapTemp.keySet()){
			prTest.println(key+","+ mapTemp.get(key));
		}
		pr.close();
		prTest.close();
	}
}
