package com.fpt.ftel.paytv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class ErrorChecking {

	private Set<String> setUserDemo;
	private Set<String> setUserActive;
	private Map<String, Map<String, DateTime>> mapUserHuy;
	private List<String> listFileLogPath;
	private Set<String> setUserNoLog;
	
	private CommonConfig cf;

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		ErrorChecking errorChecking = new ErrorChecking();
		errorChecking.filterLogForCheck();
		System.out.println("DONE");
	}

	public ErrorChecking() throws IOException {
		cf = CommonConfig.getInstance();
		loadListFilePath(new File(cf.get(CommonConfig.MAIN_DIR) + "/log_parsed"));
		System.out.println("NUMBER OF FILE: " + listFileLogPath.size());
	}

	private void loadListUserNoLog() throws IOException {
		setUserNoLog = new HashSet();
		BufferedReader br = new BufferedReader(new FileReader(cf.get(CommonConfig.MAIN_DIR) + "/time_zero.csv"));
		String line = br.readLine();
		int count = 0;
		while (line != null) {
			setUserNoLog.add(line);
			count++;
			line = br.readLine();
		}
		System.out.println("LIST NO LOG: " + count);
		br.close();
	}


	private void loadListFilePath(File file) {
		if (listFileLogPath == null) {
			listFileLogPath = new ArrayList<>();
		}
		File[] subdir = file.listFiles();
		for (File f : subdir) {
			if (f.isFile()) {
				listFileLogPath.add(f.getAbsolutePath());
			}
			if (f.isDirectory()) {
				loadListFilePath(f);
			}
		}
	}

	public void checkLogDuplicate() throws IOException {
		File[] files = new File(cf.get(CommonConfig.MAIN_DIR) + "/log_parsed/t2/").listFiles();
		PrintWriter pr = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/duplicate.csv"));
		int count = 0;
		for (File file : files) {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			String oldLine = "";
			while (line != null) {
				if (line.equals(oldLine)) {
					count++;
					pr.println(oldLine);
					pr.println(line);
				}
				oldLine = line;
				line = br.readLine();
			}
			br.close();
			System.out.println("Done file: " + file.getName() + " | Count: " + count);
		}
		pr.close();
	}

	public void checkDanhSachHuyActive() throws IOException {
		Set<String> setMissing = new HashSet<>();
		PrintWriter prLog = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/log_missing.csv"));
		PrintWriter prUser = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/user_missing.csv"));
		for (String file : listFileLogPath) {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {
					String customerId = arr[0];
					if (!customerId.isEmpty() && !customerId.equals("null") && !setUserActive.contains(customerId)
							&& !mapUserHuy.containsKey(customerId) && !setUserDemo.contains(customerId)) {
						setMissing.add(customerId);
						prLog.println(line);
					}
				} else {
					System.out.println("Line Error: " + line);
				}
				line = br.readLine();
			}
			br.close();
			System.out.println("Done file " + file + " | " + setMissing.size());
		}
		for (String customerId : setMissing) {
			prUser.println(customerId);
		}
		prLog.close();
		prUser.close();
	}

	public void filterLogForCheck() throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/log.csv"));
		// File[] files = new File(Utils.DIR + "log_parsed/t3/").listFiles();
		for (String file : listFileLogPath) {
			long start = System.currentTimeMillis();
			// System.out.println(file);
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {
					if (arr[0].equals("491592")) {
						Double realTimePlaying = PayTVUtils.parseRealTimePlaying(arr[5]);
						if (realTimePlaying != null) {
							pr.println(line);
						}
					}
				} else {
					System.out.println("Log error: " + line);
				}
				line = br.readLine();
			}
			br.close();
			System.out.println("Done filter file: " + file + " | Time: " + (System.currentTimeMillis() - start));
		}
		pr.close();
	}

	public void getUserActiveNolog() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(cf.get(CommonConfig.MAIN_DIR) + "/unique_customer.csv"));
		String line = br.readLine();
		Set<String> setUnique = new HashSet<>();
		while (line != null) {
			setUnique.add(line);
			line = br.readLine();
		}
		br.close();
		int count = 0;
		PrintWriter pr = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/userHasNoLog.csv"));
		for (String c : setUserNoLog) {
			if (!setUnique.contains(c)) {
				pr.println(c);
				count++;
			}
		}
		pr.close();
		System.out.println(count);
	}

}
