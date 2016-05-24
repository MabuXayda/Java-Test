package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;

import com.fpt.tv.utils.SupportDataUtils;
import com.fpt.tv.utils.Utils;

public class ErrorChecking {

	private Set<String> setUserDemo;
	private Set<String> setUserActive;
	private Map<String, Map<String, DateTime>> mapUserHuy;
	private Map<String, DateTime> mapUserCondition;
	private List<String> listFileLogPath;
	private Set<String> setUserNoLog;

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		ErrorChecking errorChecking = new ErrorChecking();
		errorChecking.filterLogForCheck();
		System.out.println("DONE");
	}

	public ErrorChecking() throws IOException {
		loadListFilePath(new File(Utils.DIR + "log_parsed"));
		System.out.println("NUMBER OF FILE: " + listFileLogPath.size());
		loadSupportData();
	}

	private void loadListUserNoLog() throws IOException {
		setUserNoLog = new HashSet();
		BufferedReader br = new BufferedReader(new FileReader(Utils.DIR + "time_zero.csv"));
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

	private void loadSupportData() throws IOException {
		if (mapUserCondition == null) {
			mapUserCondition = new HashMap<>();
		}

		setUserDemo = SupportDataUtils.loadSetUserDemo();
		System.out.println("NUMBER USER DEMO: " + setUserDemo.size());

		// load map User_huy + Day_condition
		mapUserHuy = SupportDataUtils.loadMapUserHuy();
		for (String customerId : mapUserHuy.keySet()) {
			DateTime startDate = mapUserHuy.get(customerId).get("start");
			DateTime stopDate = mapUserHuy.get(customerId).get("stop");
			Duration duration = new Duration(startDate, stopDate);
			int daysActive = (int) duration.getStandardDays();
			if (daysActive >= 28) {
				mapUserCondition.put(customerId, stopDate);
			}
		}
		System.out.println("NUMBER USER HUY: " + mapUserHuy.size());

		// load map User_active + Day_condition
		setUserActive = SupportDataUtils.loadSetUserActive(mapUserHuy);
		DateTime dateCondition = DateTimeFormat.forPattern("dd/MM/yyyy").parseDateTime("31/03/2016");
		for (String customerId : setUserActive) {
			mapUserCondition.put(customerId, dateCondition);
		}
		System.out.println("NUMBER USER ACTIVE: " + setUserActive.size());

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
		File[] files = new File(Utils.DIR + "log_parsed/t2/").listFiles();
		PrintWriter pr = new PrintWriter(new FileWriter(Utils.DIR + "duplicate.csv"));
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
		PrintWriter prLog = new PrintWriter(new FileWriter(Utils.DIR + "log_missing.csv"));
		PrintWriter prUser = new PrintWriter(new FileWriter(Utils.DIR + "user_missing.csv"));
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
		PrintWriter pr = new PrintWriter(new FileWriter(Utils.DIR + "log.csv"));
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
						Double realTimePlaying = Utils.parseRealTimePlaying(arr[5]);
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
		BufferedReader br = new BufferedReader(new FileReader(Utils.DIR + "unique_customer.csv"));
		String line = br.readLine();
		Set<String> setUnique = new HashSet<>();
		while (line != null) {
			setUnique.add(line);
			line = br.readLine();
		}
		br.close();
		int count = 0;
		PrintWriter pr = new PrintWriter(new FileWriter(Utils.DIR + "userHasNoLog.csv"));
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
