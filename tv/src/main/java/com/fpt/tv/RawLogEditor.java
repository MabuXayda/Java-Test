package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.fpt.tv.object.raw.Source;
import com.fpt.tv.utils.SupportDataUtils;
import com.fpt.tv.utils.Utils;
import com.google.gson.Gson;

public class RawLogEditor {

	private Set<String> setCustomerId;
	private Set<String> setCustomerIdChurnOld;
	
	
	public static void main(String[] args) throws IOException {
		RawLogEditor rawLogEditor = new RawLogEditor();
		rawLogEditor.parseLog("your_path_here");
	}

	public RawLogEditor() throws IOException {
		loadSetCustomerId();
		loadSetCustomerIdChurnOld();
	}

	private void loadSetCustomerIdChurnOld() throws IOException {
		if (setCustomerIdChurnOld == null) {
			setCustomerIdChurnOld = new HashSet<>();
		}
		BufferedReader br = new BufferedReader(new FileReader(Utils.DIR + "support/churnUser.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 5) {
				if (Utils.isNumeric(arr[0])) {
					setCustomerIdChurnOld.add(arr[0]);
				}
			} else {
				System.out.println("Load user huy old error: " + line);
			}
			line = br.readLine();
		}
		br.close();
	}
	private void loadSetCustomerId() throws IOException {
		if (setCustomerId == null) {
			setCustomerId = new HashSet<>();
		}
		BufferedReader br = new BufferedReader(new FileReader(Utils.DIR + "support/active_t4.csv"));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 4) {
				if (Utils.isNumeric(arr[0])) {
					setCustomerId.add(arr[0]);
				}
			} else {
				System.out.println("Load user active error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		br = new BufferedReader(new FileReader(Utils.DIR + "support/huy_t4.csv"));
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 5) {
				if (Utils.isNumeric(arr[0])) {
					setCustomerId.add(arr[0]);
				}
			} else {
				System.out.println("Load user huy error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Total customerId: " + setCustomerId.size());
	}

	public void parseLog(String DIR) throws IOException {
		File[] files = new File(DIR).listFiles();
		File theDir = new File(DIR + "log_parsed");
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}

		File theDirDrop = new File(DIR + "log_parsed_drop");
		if (!theDirDrop.exists()) {
			try {
				theDirDrop.mkdir();
			} catch (SecurityException se) {
			}
		}
		
		Set<String> setNonsense = new HashSet<>();

		for (File file : files) {
			int count = 0;
			int valid = 0;
			int drop = 0;

			String output = DIR + "log_parsed/" + file.getName().split("\\.")[0] + "_parsed.csv";
			PrintWriter pr = new PrintWriter(new FileWriter(output));
			pr.println("CustomerId,Contract,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			String outputDrop = DIR + "log_parsed_drop/" + file.getName().split("\\.")[0] + "_parsed_drop.csv";
			PrintWriter prDrop = new PrintWriter(new FileWriter(outputDrop));
			pr.println("CustomerId,Contract,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				if (!line.isEmpty()) {
					Source source = new Gson().fromJson(line, Source.class);
					String customerId = source.getFields().getCustomerId();
					String contract = source.getFields().getContract();
					String logId = source.getFields().getLogId();
					String appName = source.getFields().getAppName();
					String itemId = source.getFields().getItemId();
					String realTimePlaying = source.getFields().getRealTimePlaying();
					String sessionMainMenu = source.getFields().getSessionMainMenu();
					String boxTime = source.getFields().getBoxTime();
					String received_at = source.getReceived_at();
					if (customerId != null && setCustomerId.contains(customerId)) {
						if (logId != null && appName != null && sessionMainMenu != null) {
							pr.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId + ","
									+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at);
							valid++;

							if(setCustomerIdChurnOld.contains(customerId)){
								setNonsense.add(customerId);
							}
							
						} else {
							prDrop.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId
									+ "," + realTimePlaying + "," + sessionMainMenu + "," + boxTime + ","
									+ received_at);
							drop++;
						}
					} else {
						prDrop.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId + ","
								+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at);
						drop++;
					}
				}

				count++;
				if (count % 1000000 == 0) {
					System.out.println(
							file.getName().split("\\.")[0] + "Valid: " + valid + "Drop: " + drop + "Total: " + count);
				}
				line = br.readLine();
			}

			System.out.println("Done parse file: " + file.getName().split("\\.")[0] + "Valid: " + valid + "Drop: "
					+ drop + "Total: " + count);
			Utils.LOG_INFO.info("Done parse file: " + file.getName().split("\\.")[0] + "Valid: " + valid + "Drop: "
					+ drop + "Total: " + count);
			pr.close();
			prDrop.close();
			br.close();
		}
		
		System.out.println("Set nonsense size: " + setNonsense.size());
	}

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
