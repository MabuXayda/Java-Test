package com.fpt.ftel.raw;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.object.raw.Source;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class ParseLogService {

	public static void main(String[] args) {
		ParseLogService parseLogService = new ParseLogService();
		parseLogService.getProcessPath();
	}
	
	public void parseRawLogHdfs() throws JsonIOException, JsonSyntaxException, FileNotFoundException{
		Set<String> setUserSpecial = UserStatus.getListUserSpecial(CommonConfig.getInstance().get(CommonConfig.USER_SPECIAL_FILE));
		getProcessPath();
		String path = CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_HDFS_DIR) + getProcessPath();
		List<File> listFile = Arrays.asList(new File(CommonConfig.getInstance().get(CommonConfig.RAW_LOG_DIR) + getProcessPath()).listFiles());
		FileUtils.sortListFileNumber(listFile);
		
		
		for(File file : listFile){
			
		}
	}

	public String getProcessPath() {
		DateTime dateTime = new DateTime();
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = year + File.separator + NumberUtils.getTwoCharNumber(month) + File.separator
				+ NumberUtils.getTwoCharNumber(day) + File.separator + NumberUtils.getTwoCharNumber(hour);
		return path;
	}

	private void hashRawData() throws IOException, NoSuchAlgorithmException {
		List<File> listFile = Arrays
				.asList(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4").listFiles());
		FileUtils.sortListFileDateTime(listFile);
		String hashFolder = CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4_hash";
		FileUtils.createFolder(hashFolder);

		Map<String, String> mapHashCode = new HashMap<>();
		BufferedReader br_h = new BufferedReader(
				new FileReader(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/hash/hash_old.csv"));
		String line_h = br_h.readLine();
		while (line_h != null) {
			String[] arr = line_h.split(",");
			mapHashCode.put(arr[0], arr[1]);
			line_h = br_h.readLine();
		}
		br_h.close();
		System.out.println("Total hash: " + mapHashCode.size());

		for (File file : listFile) {
			long start = System.currentTimeMillis();
			int count = 0;
			int valid = 0;

			BufferedReader br = new BufferedReader(new FileReader(file));
			PrintWriter pr = new PrintWriter(new FileWriter(hashFolder + "/" + file.getName()));
			pr.println("CustomerId,LogId,AppName,ItemId,RealTimePlaying,SessionMainMenu,BoxTime,received_at");

			String line = br.readLine();
			while (line != null) {
				String[] arr = line.split(",");
				if (arr.length == 9) {
					String customerId = arr[0];
					String logId = arr[2];
					String appName = arr[3];
					String itemId = arr[4];
					String realTimePlaying = arr[5];
					String sessionMainMenu = null;
					if (arr[6].length() == 36) {
						sessionMainMenu = arr[6].substring(13);
					}
					String boxTime = arr[7];
					String received_at = arr[8];
					String customerIdHash;

					if (StringUtils.isNumeric(customerId) && StringUtils.isNumeric(logId)) {
						if (received_at != null && !received_at.equals("null") && !received_at.isEmpty()) {
							if (mapHashCode.containsKey(customerId)) {
								customerIdHash = mapHashCode.get(customerId);
							} else {
								customerIdHash = StringUtils.hashCode(customerId);
								mapHashCode.put(customerId, customerIdHash);
							}

							pr.println(customerIdHash + "," + logId + "," + appName + "," + itemId + ","
									+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at);

							valid++;
						}

					}

				} else {
					PayTVUtils.LOG_ERROR.error("Parsed log error: " + line);
				}
				line = br.readLine();
				count++;
				if (count % 500000 == 0) {
					System.out.println(file.getName() + " | " + count);
				}
			}
			PayTVUtils.LOG_INFO.info("Done hash: " + file.getName() + " | Valid/Total | " + valid + "/" + count
					+ " | Time: " + (System.currentTimeMillis() - start));
			br.close();
			pr.close();
		}

		PrintWriter pr = new PrintWriter(
				new FileWriter(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/Hash.csv"));
		for (String customerId : mapHashCode.keySet()) {
			pr.println(customerId + "," + mapHashCode.get(customerId));
		}
		pr.close();
	}

	public void parseRawLog() throws IOException {
		File[] files = new File("/media/tunn/Elements/logs/t5/process").listFiles();
		File theDir = new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR));
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException se) {
			}
		}

		for (File file : files) {
			System.out.println("Process file: " + file.getName());
			long start = System.currentTimeMillis();
			int count = 0;
			int valid = 0;
			int drop = 0;

			String output = CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/"
					+ file.getName().split("\\.")[0] + "_parsed.csv";
			PrintWriter pr = new PrintWriter(new FileWriter(output));
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

						if (StringUtils.isNumeric(customerId) && logId != null && appName != null) {
							pr.println(customerId + "," + contract + "," + logId + "," + appName + "," + itemId + ","
									+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at);
							valid++;
						} else {
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
			PayTVUtils.LOG_INFO.info("Done parse file: " + file.getName().split("\\.")[0] + " | Valid: " + valid
					+ " | Drop: " + drop + " | Total: " + count + " | Time: " + (System.currentTimeMillis() - start));
			pr.close();
			br.close();
		}
	}
}
