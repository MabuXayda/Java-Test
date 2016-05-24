package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.fpt.tv.object.raw.Source;
import com.fpt.tv.utils.SupportDataUtils;
import com.fpt.tv.utils.Utils;
import com.google.gson.Gson;

public class RawLogEditor {
	
	public void parseRawLog() throws IOException {
		Set<String> setUserNghiepVu = SupportDataUtils.loadSetUserNghiepVu();
		File[] files = new File(Utils.DIR + "log/").listFiles();
		File theDir = new File(Utils.DIR + "log_parsed");
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}
		AtomicInteger count = new AtomicInteger(0);
		AtomicInteger countNV = new AtomicInteger(0);
		AtomicInteger countError = new AtomicInteger(0);
		for (File file : files) {
			long start = System.currentTimeMillis();
			// tao file ghi

			String output = Utils.DIR + "log_parsed/" + file.getName().split("\\.")[0] + "_parsed.csv";

			PrintWriter pr = new PrintWriter(new FileWriter(output));
			pr.println("CustomerId,Contract,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			// tao threadpool
			ExecutorService executorService = Executors.newFixedThreadPool(20);

			// bat dau doc file
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				// System.out.println(line);
				final String gsonLine = line;
				Runnable runnable = new Runnable() {
					@Override
					public void run() {
						if (gsonLine != null && !gsonLine.isEmpty()) {
							Source source = new Gson().fromJson(gsonLine, Source.class);
							try {
								String customerId = source.getFields().getCustomerId();
								if (!setUserNghiepVu.contains(customerId)) {
									String contract = source.getFields().getContract();
									String logId = source.getFields().getLogId();
									String appName = source.getFields().getAppName();
									String itemId = source.getFields().getItemId();
									String realTimePlaying = source.getFields().getRealTimePlaying();
									String sessionMainMenu = source.getFields().getSessionMainMenu();
									String boxTime = source.getFields().getBoxTime();
									String received_at = source.getReceived_at();
									if (Utils.isNumeric(customerId)) {
										pr.println(customerId + "," + contract + "," + logId + "," + appName + ","
												+ itemId + "," + realTimePlaying + "," + sessionMainMenu + "," + boxTime
												+ "," + received_at);
									}
									count.incrementAndGet();
									if (count.get() % 100000 == 0) {
										System.out.println(count);
									}
								} else {
									countNV.incrementAndGet();
								}
							} catch (Exception e) {
								countError.incrementAndGet();
								Utils.LOG_ERROR.error("Log error : " + gsonLine);
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
			Utils.LOG_INFO.info("===> Done file: " + file.getName());
			Utils.LOG_INFO.info("===> Count: " + count + " | CountNV: " + countNV + " | CountError: " + countError
					+ " | Time: " + (end - start));
			count.set(0);
			countNV.set(0);
			countError.set(0);
			br.close();
			pr.close();
		}
	}
}
