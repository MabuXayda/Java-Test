package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fpt.tv.utils.Utils;

public class SupportOther {

	public static void main(String[] args) throws IOException {
		SupportOther supportOther = new SupportOther();
		System.out.println("START");
		supportOther.filterLog();
		System.out.println("DONE");
	}

	private Set<String> loadSetUserActiveSample() throws IOException {
		Set<String> setUserActive = new HashSet<>();
		BufferedReader br = new BufferedReader(new FileReader(Utils.DIR + "modeId.csv"));
		String line = br.readLine();
		while (line != null) {
			if (line != null && !line.isEmpty()) {
				setUserActive.add(line);
			} else {
				System.out.println("User Active Sample Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		return setUserActive;
	}

	private Set<String> loadSetUserHuySample() throws IOException {
		Set<String> setUserHuy = new HashSet<>();
		BufferedReader br = new BufferedReader(new FileReader(Utils.DIR + "modeHuy.csv"));
		String line = br.readLine();
		while (line != null) {
			if (line != null && !line.isEmpty()) {
				setUserHuy.add(line);
			} else {
				System.out.println("User Active Sample Error: " + line);
			}
			line = br.readLine();
		}
		br.close();
		return setUserHuy;
	}

	public void filterLog() throws IOException {
		Set<String> setUserActive = loadSetUserActiveSample();
		Set<String> setUserHuy = loadSetUserHuySample();
		File[] files = new File(Utils.DIR + "log_parsed/t2/").listFiles();
		PrintWriter prActive = new PrintWriter(new FileWriter(Utils.DIR + "t2_ActiveUserLog.csv"));
		PrintWriter prHuy = new PrintWriter(new FileWriter(Utils.DIR + "t2_HuyUserLog.csv"));
		ExecutorService executorService = Executors.newFixedThreadPool(20);
		for (File file : files) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					int count = 0;
					BufferedReader br = null;
					try {
						br = new BufferedReader(new FileReader(file));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					String line = null;
					try {
						line = br.readLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					prActive.println(line);
					prHuy.println(line);
					try {
						line = br.readLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					while (line != null) {
						String[] arr = line.split(",");
						if (arr.length == 9) {
							String customerId = arr[0];
							if (setUserActive.contains(customerId)) {
								prActive.println(line);
							}
							if (setUserHuy.contains(customerId)) {
								prHuy.println(line);
							}
						} else {
							Utils.LOG_ERROR.error("Parse log error: " + line);
						}
						try {
							line = br.readLine();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						count++;
						if (count % 500000 == 0) {
							System.out.println(count + " | " + file.getName());
						}
					}
					try {
						br.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					Utils.LOG_INFO.info("Done support file: " + file.getName() + " | Count: " + count
							+ " | Time: " + (System.currentTimeMillis() - start));
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		prActive.close();
		prHuy.close();
	}
}
