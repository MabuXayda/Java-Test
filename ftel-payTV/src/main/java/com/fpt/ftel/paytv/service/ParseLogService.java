package com.fpt.ftel.paytv.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIOSimple;
import com.fpt.ftel.paytv.object.raw.Source;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class ParseLogService {
	private HdfsIOSimple hdfsIOSimple;
	private static final String PROCESS_MISSING = "./process_missing";
	private Set<String> setUserSpecial;
	private CommonConfig cf;

	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, IOException {
		ParseLogService parseLogService = new ParseLogService();
		if (args[0].equals("fix") && args.length == 3) {
			System.out.println("Start parse log fix job ..........");
			parseLogService.processParseRawLogHdfsFix(args[1], args[2]);
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start parse log real job ..........");
			parseLogService.processParseRawLogHdfsReal(args[1]);
		}
	}

	public ParseLogService() throws IOException {
		cf = CommonConfig.getInstance();
		PropertyConfigurator.configure(cf.get(CommonConfig.LOG4J_CONFIG_DIR) + "/log4j_ParseLogService.properties");
		hdfsIOSimple = new HdfsIOSimple();
		if (hdfsIOSimple.isExist(cf.get(CommonConfig.PARSED_LOG_HDFS_DIR))) {
			hdfsIOSimple.createFolder(cf.get(CommonConfig.PARSED_LOG_HDFS_DIR));
		}
		setUserSpecial = UserStatus.getSetUserSpecial(cf.get(CommonConfig.USER_SPECIAL_FILE));
	}

	public void processParseRawLogHdfsReal(String dateString) throws IOException {
		List<String> listDateString = getListParseMissing();
		List<String> listMissing = new ArrayList<>();
		DateTime currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(currentDateTime.minusMinutes(5)));
		for (String date : listDateString) {
			currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);
			if (FileUtils.isExistFile(getProcessFilePath(currentDateTime.plusMinutes(5)))) {
				parseRawLogHdfs(getProcessFilePath(currentDateTime), setUserSpecial);
			} else {
				listMissing.addAll(listDateString.subList(listDateString.indexOf(date), listDateString.size()));
				break;
			}
		}
		printListParseMissing(listMissing);
	}

	public void processParseRawLogHdfsFix(String fromDate, String toDate) throws IOException {
		List<DateTime> listDateFix = getListDateFix(fromDate, toDate);
		for (DateTime date : listDateFix) {
			String processPath = getProcessDirPath(date);
			List<String> listFilePath = FileUtils.getListFilePath(new File(processPath));
			FileUtils.sortListFilePathNumber(listFilePath);
			for (String filePath : listFilePath) {
				parseRawLogHdfs(filePath, setUserSpecial);
			}
		}
	}

	public static List<String> getListParseMissing() throws IOException {
		List<String> listMissing = new ArrayList<>();
		if (FileUtils.isExistFile(PROCESS_MISSING)) {
			BufferedReader br = new BufferedReader(new FileReader(PROCESS_MISSING));
			String line = br.readLine();
			while (line != null) {
				listMissing.add(line);
				line = br.readLine();
			}
			br.close();
		}
		return listMissing;
	}

	public static void printListParseMissing(List<String> listMissing) throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(PROCESS_MISSING));
		if (listMissing.size() == 0) {
			pr.print("");
		} else {
			for (String fileMissing : listMissing) {
				pr.println(fileMissing);
			}
		}
		pr.close();
	}

	private String getProcessFilePath(DateTime date) {
		int year = date.getYear();
		int month = date.getMonthOfYear();
		int day = date.getDayOfMonth();
		int hour = date.getHourOfDay();
		int index = (int) Math.ceil(date.getMinuteOfHour() / 5.0) - 1;
		String filePath = year + "/" + NumberUtils.getTwoCharNumber(month) + "/" + NumberUtils.getTwoCharNumber(day)
				+ "/" + NumberUtils.getTwoCharNumber(hour) + "/fbox_" + index + ".txt";
		return cf.get(CommonConfig.RAW_LOG_DIR) + "/" + filePath;
	}

	private String getProcessDirPath(DateTime dateTime) {
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = year + File.separator + NumberUtils.getTwoCharNumber(month) + File.separator
				+ NumberUtils.getTwoCharNumber(day) + File.separator + NumberUtils.getTwoCharNumber(hour);
		return cf.get(CommonConfig.RAW_LOG_DIR) + "/" + path;
	}

	private List<DateTime> getListDateFix(String fromDate, String toDate) {
		List<DateTime> listDateFix = new ArrayList<>();
		DateTime beginDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(fromDate);
		DateTime endDate = PayTVUtils.FORMAT_DATE_TIME_HOUR.parseDateTime(toDate);
		while (new Duration(beginDate, endDate).getStandardSeconds() > 0) {
			listDateFix.add(beginDate);
			beginDate = beginDate.plusHours(1);
		}
		return listDateFix;
	}

	private void parseRawLogHdfs(String filePath, Set<String> setUserSpecial) throws IOException {
		List<String> listLog = new ArrayList<>();
		int startYear = 2016;
		int startMonth = 2;
		int startDay = 1;
		int startHour = 1;
		int countPrint = 0;
		int countTotal = 0;
		long start = System.currentTimeMillis();
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = br.readLine();

		while (line != null) {
			countTotal++;

			if (!line.isEmpty()) {
				try {
					Source source = new Gson().fromJson(line, Source.class);
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
					String ip_wan = source.getFields().getIp_wan();

					if (StringUtils.isNumeric(customerId) && logId != null && appName != null) {
						String writeLog = customerId + "," + contract + "," + logId + "," + appName + "," + itemId + ","
								+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at_string
								+ "," + ip_wan;

						if (received_at != null) {
							int year = received_at.getYear();
							int month = received_at.getMonthOfYear();
							int day = received_at.getDayOfMonth();
							int hour = received_at.getHourOfDay();
							if (StringUtils.isNumeric(customerId) && !setUserSpecial.contains(customerId)) {
								if (year == startYear && month == startMonth && day == startDay && hour == startHour) {
									listLog.add(writeLog);
								} else {
									countPrint = printParsedLogHdfs(startYear, startMonth, startDay, startHour, listLog,
											countPrint);

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

		countPrint = printParsedLogHdfs(startYear, startMonth, startDay, startHour, listLog, countPrint);
		listLog = new ArrayList<>();

		PayTVUtils.LOG_INFO.info("Done file: " + filePath + " | Print: " + countPrint + " | Total: " + countTotal
				+ " | Time: " + (System.currentTimeMillis() - start));
	}

	private int printParsedLogHdfs(int year, int month, int day, int hour, List<String> listLog, int count)
			throws IOException {

		String path = cf.get(CommonConfig.PARSED_LOG_HDFS_DIR) + "/" + year;
		path = path + "/" + String.format("%02d", month);
		path = path + "/" + String.format("%02d", day);
		path = path + "/" + String.format("%02d", hour);
		if (!hdfsIOSimple.isExist(path)) {
			hdfsIOSimple.createFolder(path);
		}
		path = path + "/" + year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "_"
				+ String.format("%02d", hour) + "_log_parsed.csv";
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
				count++;
			}
			bw.close();
		}

		return count;
	}

	public void hashCustomerId() throws IOException, NoSuchAlgorithmException {
		List<File> listFile = Arrays.asList(new File(cf.get(CommonConfig.PARSED_LOG_DIR) + "/t4").listFiles());
		FileUtils.sortListFileDateTime(listFile);
		String hashFolder = cf.get(CommonConfig.PARSED_LOG_DIR) + "/t4_hash";
		FileUtils.createFolder(hashFolder);

		Map<String, String> mapHashCode = new HashMap<>();
		BufferedReader br_h = new BufferedReader(new FileReader(cf.get(CommonConfig.MAIN_DIR) + "/hash/hash_old.csv"));
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

						pr.println(customerIdHash + "," + logId + "," + appName + "," + itemId + "," + realTimePlaying
								+ "," + sessionMainMenu + "," + boxTime + "," + received_at);

						valid++;
					}

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

		PrintWriter pr = new PrintWriter(new FileWriter(cf.get(CommonConfig.MAIN_DIR) + "/Hash.csv"));
		for (String customerId : mapHashCode.keySet()) {
			pr.println(customerId + "," + mapHashCode.get(customerId));
		}
		pr.close();
	}

}
