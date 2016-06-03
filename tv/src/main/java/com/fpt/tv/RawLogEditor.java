package com.fpt.tv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import com.fpt.tv.object.raw.Source;
import com.fpt.tv.utils.CommonConfig;
import com.fpt.tv.utils.Utils;
import com.google.gson.Gson;

public class RawLogEditor {

	private Set<String> setCustomerId;
	private Set<String> setCustomerIdChurnOld;
	private CommonConfig cf;

	public static void main(String[] args) throws IOException {
		RawLogEditor rawLogEditor = new RawLogEditor();
		rawLogEditor.parseRawLog();
	}

	public RawLogEditor() throws IOException {
		loadSetCustomerId();
		loadSetCustomerIdChurnOld();
		cf = CommonConfig.getInstance();
	}

	private void loadSetCustomerIdChurnOld() throws IOException {
		if (setCustomerIdChurnOld == null) {
			setCustomerIdChurnOld = new HashSet<>();
		}
		BufferedReader br = new BufferedReader(
				new FileReader(cf.get(CommonConfig.SUPPORT_DATA_DIR) + "/churnUser.csv"));
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
		BufferedReader br = new BufferedReader(
				new FileReader(cf.get(CommonConfig.SUPPORT_DATA_DIR) + "/active_t4.csv"));
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
		br = new BufferedReader(new FileReader(cf.get(CommonConfig.SUPPORT_DATA_DIR) + "/huy_t4.csv"));
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

	public void parseRawLog() throws IOException {
		File[] files = new File(cf.get(CommonConfig.RAW_LOG_DIR)).listFiles();
		File theDir = new File(cf.get(CommonConfig.PARSED_LOG_DIR));
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}

		File theDirDrop = new File(cf.get(CommonConfig.PARSED_LOG_DROP_DIR));
		if (!theDirDrop.exists()) {
			try {
				theDirDrop.mkdir();
			} catch (SecurityException se) {
			}
		}

		Set<String> setNonsense = new HashSet<>();

		for (File file : files) {
			System.out.println("Process file: " + file.getName());
			long start = System.currentTimeMillis();
			int count = 0;
			int valid = 0;
			int drop = 0;

			String output = cf.get(CommonConfig.PARSED_LOG_DIR) + "/" + file.getName().split("\\.")[0]
					+ "_parsed.csv";
			PrintWriter pr = new PrintWriter(new FileWriter(output));
			pr.println("CustomerId,Contract,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			String outputDrop = cf.get(CommonConfig.PARSED_LOG_DROP_DIR) + "/" + file.getName().split("\\.")[0]
					+ "_parsed_drop.csv";
			PrintWriter prDrop = new PrintWriter(new FileWriter(outputDrop));
			pr.println("CustomerId,Contract,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				if (!line.isEmpty()) {
					Source source = new Gson().fromJson(line, Source.class);
					try {
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
								pr.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId
										+ "," + realTimePlaying + "," + sessionMainMenu + "," + boxTime + ","
										+ received_at);
								valid++;

								if (setCustomerIdChurnOld.contains(customerId)) {
									setNonsense.add(customerId);
								}

							} else {
								prDrop.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId
										+ "," + realTimePlaying + "," + sessionMainMenu + "," + boxTime + ","
										+ received_at);
								drop++;
							}
						} else {
							prDrop.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId
									+ "," + realTimePlaying + "," + sessionMainMenu + "," + boxTime + ","
									+ received_at);
							drop++;
						}
					} catch (Exception e) {
						// TODO: handle exception
					}
				}

				count++;
				if (count % 1000000 == 0) {
					System.out.println(file.getName().split("\\.")[0] + " | Valid: " + valid + " | Drop: " + drop
							+ " | Total: " + count);
				}
				line = br.readLine();
			}

			System.out.println("Done parse file: " + file.getName().split("\\.")[0] + " | Valid: " + valid + " | Drop: "
					+ drop + " | Total: " + count + " | Time: " + (System.currentTimeMillis() - start));
			Utils.LOG_INFO.info("Done parse file: " + file.getName().split("\\.")[0] + " | Valid: " + valid
					+ " | Drop: " + drop + " | Total: " + count + " | Time: " + (System.currentTimeMillis() - start));
			pr.close();
			prDrop.close();
			br.close();

		}

		System.out.println("Set nonsense size: " + setNonsense.size());
	}

}
