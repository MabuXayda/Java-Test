package com.fpt.ftel.raw;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIOSimple;
import com.fpt.ftel.paytv.object.raw.Source;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class LocalToHDFS {

	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, IOException {
		long start = System.currentTimeMillis();
		PayTVUtils.LOG_INFO.info("=============== Start job move log at: " + start);
		LocalToHDFS localToHDFS = new LocalToHDFS();
		localToHDFS.copyFromLocalRaw();
		PayTVUtils.LOG_INFO.info("=============== Done job move log at: " + (System.currentTimeMillis() - start));
	}

	public void copyFromLocal() throws JsonIOException, JsonSyntaxException, IOException {
		HdfsIOSimple hdfsIOSimple = new HdfsIOSimple();
		Set<String> listUserSpecial = UserStatus.getSetUserSpecial(
				CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/paytv_get_dm_nv_vip");
		List<String> listFilePath = FileUtils
				.getListFilePath(new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t5"));
		FileUtils.sortListFilePathDateTime(listFilePath);
		List<String> listLog = new ArrayList<>();
		String outputDir = "/data/payTV/raw";
		hdfsIOSimple.createFolder(outputDir);

		int startYear = 2016;
		int startMonth = 2;
		int startDay = 1;
		int startHour = 1;

		for (String filePath : listFilePath) {
			int countPrint = 0;
			int countTotal = 0;
			long startCopy = System.currentTimeMillis();
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = br.readLine();
			while (line != null) {
				countTotal++;
				String[] arr = line.split(",");
				String customerId = arr[0];
				DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);

				if (received_at != null) {

					int year = received_at.getYear();
					int month = received_at.getMonthOfYear();
					int day = received_at.getDayOfMonth();
					int hour = received_at.getHourOfDay();

					if (StringUtils.isNumeric(customerId) && !listUserSpecial.contains(customerId)) {
						if (year == startYear && month == startMonth && day == startDay && hour == startHour) {
							listLog.add(line);
						} else {

							String path = outputDir + "/" + startYear;
							path = path + "/" + String.format("%02d", startMonth);
							path = path + "/" + String.format("%02d", startDay);
							path = path + "/" + String.format("%02d", startHour);
							if (!hdfsIOSimple.isExist(path)) {
								hdfsIOSimple.createFolder(path);
							}
							path = path + "/2016-" + String.format("%02d", startMonth) + "-"
									+ String.format("%02d", startDay) + "_" + String.format("%02d", startHour)
									+ "_log_parsed.csv";
							// int index = 0;
							// String pathIndex = path + "_" + index + ".csv";
							// while (hdfsIOSimple.isExist(pathIndex)) {
							// index += 1;
							// pathIndex = path + "_" + index + ".csv";
							// }

							if (listLog.size() > 0) {
								System.out.println("Push data to : " + path);
								BufferedWriter bw;
								if (hdfsIOSimple.isExist(path)) {
									bw = hdfsIOSimple.getWriteStreamAppendToHdfs(path);
								} else {
									bw = hdfsIOSimple.getWriteStreamNewToHdfs(path);
								}
								for (String log : listLog) {
									bw.write(log);
									bw.write("\n");
								}
								bw.close();
							}

							startMonth = month;
							startDay = day;
							startHour = hour;
							listLog = new ArrayList<>();
							listLog.add(line);
						}
					}

				}
				line = br.readLine();
			}
			br.close();
			PayTVUtils.LOG_INFO.info("Done file: " + filePath + " | Print: " + countPrint + " | Total: " + countTotal
					+ " | Time: " + (System.currentTimeMillis() - startCopy));
		}
	}

	public void copyFromLocalRaw() throws IOException {
		HdfsIOSimple hdfsIOSimple = new HdfsIOSimple();
		Set<String> listUserSpecial = UserStatus.getSetUserSpecial(
				CommonConfig.getInstance().get(CommonConfig.SUPPORT_DATA_DIR) + "/paytv_get_dm_nv_vip");
		List<String> listFilePath = FileUtils.getListFilePath(new File("/media/tunn/Elements/logs/06"));
		FileUtils.sortListFilePathDateTime(listFilePath);
		List<String> listLog = new ArrayList<>();
		String outputDir = "/data/payTV/log_parsed";
		hdfsIOSimple.createFolder(outputDir);
		int startYear = 2016;
		int startMonth = 2;
		int startDay = 1;
		int startHour = 1;

		for (String filePath : listFilePath) {
			int countPrint = 0;
			int countTotal = 0;
			long startCopy = System.currentTimeMillis();
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = br.readLine();
			while (line != null) {
				countTotal++;

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
						String received_at_string = source.getReceived_at();
						DateTime received_at = PayTVUtils.parseReceived_at(received_at_string);

						if (StringUtils.isNumeric(customerId) && logId != null && appName != null) {
							String writeLog = customerId + "," + contract + "," + logId + "," + appName + "," + itemId
									+ "," + realTimePlaying + "," + sessionMainMenu + "," + boxTime + ","
									+ received_at_string;

							if (received_at != null) {

								int year = received_at.getYear();
								int month = received_at.getMonthOfYear();
								int day = received_at.getDayOfMonth();
								int hour = received_at.getHourOfDay();

								if (StringUtils.isNumeric(customerId) && !listUserSpecial.contains(customerId)) {
									if (year == startYear && month == startMonth && day == startDay
											&& hour == startHour) {
										listLog.add(writeLog);
									} else {

										String path = outputDir + "/" + startYear;
										path = path + "/" + String.format("%02d", startMonth);
										path = path + "/" + String.format("%02d", startDay);
										path = path + "/" + String.format("%02d", startHour);
										if (!hdfsIOSimple.isExist(path)) {
											hdfsIOSimple.createFolder(path);
										}
										path = path + "/2016-" + String.format("%02d", startMonth) + "-"
												+ String.format("%02d", startDay) + "_"
												+ String.format("%02d", startHour) + "_log_parsed.csv";
										if (listLog.size() > 0) {
											System.out.println("Push data to : " + path);
											BufferedWriter bw;
											if (hdfsIOSimple.isExist(path)) {
												bw = hdfsIOSimple.getWriteStreamAppendToHdfs(path);
											} else {
												bw = hdfsIOSimple.getWriteStreamNewToHdfs(path);
											}
											for (String log : listLog) {
												bw.write(log);
												bw.write("\n");
												countPrint ++;
											}
											bw.close();
										}

										startMonth = month;
										startDay = day;
										startHour = hour;
										listLog = new ArrayList<>();
										listLog.add(writeLog);
									}
								}
							}
						}
					} catch (Exception e) {
						// TODO: handle exception
					}
				}
				line = br.readLine();
			}
			br.close();
			PayTVUtils.LOG_INFO.info("Done file: " + filePath + " | Print: " + countPrint + " | Total: " + countTotal
					+ " | Time: " + (System.currentTimeMillis() - startCopy));
		}
	}

}
