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
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.hdfs.HdfsIO;
import com.fpt.ftel.paytv.object.raw.Fields;
import com.fpt.ftel.paytv.object.raw.Source;
import com.fpt.ftel.paytv.statistic.UserStatus;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.fpt.ftel.paytv.utils.ServiceUtils;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

public class ServiceParseLog {
	private HdfsIO hdfsIO;
	private Set<String> setUserSpecial;

	public static void main(String[] args) throws JsonIOException, JsonSyntaxException, IOException {
		ServiceParseLog parseLogService = new ServiceParseLog();
		if (args[0].equals("fix") && args.length == 3) {
			System.out.println("Start parse log fix ..........");
			parseLogService.processParseLogFix(args[1], args[2]);
		} else if (args[0].equals("real") && args.length == 2) {
			System.out.println("Start parse log real ..........");
			parseLogService.processParseLogReal(args[1]);
		}
		System.out.println("DONE " + args[0] + " job");
	}

	public ServiceParseLog() throws IOException {
		PropertyConfigurator
				.configure(CommonConfig.get(PayTVConfig.LOG4J_CONFIG_DIR) + "/log4j_ParseLogService.properties");
		hdfsIO = new HdfsIO(CommonConfig.get(PayTVConfig.HDFS_CORE_SITE), CommonConfig.get(PayTVConfig.HDFS_SITE));
		if (hdfsIO.isExist(CommonConfig.get(PayTVConfig.PARSED_LOG_HDFS_DIR))) {
			hdfsIO.createFolder(CommonConfig.get(PayTVConfig.PARSED_LOG_HDFS_DIR));
		}
		setUserSpecial = UserStatus.getSetUserSpecial(CommonConfig.get(PayTVConfig.USER_SPECIAL_FILE));
	}

	public void processParseLogReal(String dateString) throws IOException {
		List<String> listDateString = ServiceUtils.getListProcessMissing(ServiceUtils.PARSE_LOG_SERVICE_MISSING);
		List<String> listMissing = new ArrayList<>();
		DateTime currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		listDateString.add(PayTVUtils.FORMAT_DATE_TIME.print(currentDateTime.minusMinutes(5)));

		DateTime markDate = null;
		for (String date : listDateString) {
			currentDateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(date);
			if (FileUtils.isExistFile(getFilePathFromDateTime(currentDateTime.plusMinutes(5)))) {
				if (markDate == null || DateTimeUtils.compareToHour(markDate, currentDateTime) == -1) {
					parseLog(currentDateTime);
				} else {
					listMissing.add(date);
				}
			} else {
				listMissing.add(date);
				markDate = currentDateTime;
			}
		}

		ServiceUtils.printListProcessMissing(listMissing, ServiceUtils.PARSE_LOG_SERVICE_MISSING);
	}

	public void processParseLogFix(String fromDate, String toDate) throws IOException {
		List<DateTime> listDateFix = getListDateFix(fromDate, toDate);
		for (DateTime date : listDateFix) {
			String processPath = getDirPathFromDateTime(date);
			List<String> listFilePath = FileUtils.getListFilePath(new File(processPath));
			FileUtils.sortListFilePathNumber(listFilePath);
			for (String filePath : listFilePath) {
				parseLog(getDateTimeFromFilePath(filePath));
			}
		}
	}

	private DateTime getDateTimeFromFilePath(String filePath) {
		String arr[] = filePath.split("/");
		String minute = Integer.toString(Integer.parseInt(arr[arr.length - 1].split("\\.")[0].split("_")[1]) * 5);
		String hour = arr[arr.length - 2];
		String day = arr[arr.length - 3];
		String month = arr[arr.length - 4];
		String year = arr[arr.length - 5];

		return PayTVUtils.FORMAT_DATE_TIME
				.parseDateTime(year + "-" + month + "-" + day + " " + hour + ":" + minute + ":00").plusMinutes(1);
	}

	public String getFilePathFromDateTime(DateTime date) {
		int year = date.getYear();
		int month = date.getMonthOfYear();
		int day = date.getDayOfMonth();
		int hour = date.getHourOfDay();
		int index = (int) Math.ceil(date.getMinuteOfHour() / 5.0) - 1;
		String filePath = year + "/" + NumberUtils.get2CharNumber(month) + "/" + NumberUtils.get2CharNumber(day) + "/"
				+ NumberUtils.get2CharNumber(hour) + "/fbox_" + index + ".txt";
		return CommonConfig.get(PayTVConfig.RAW_LOG_DIR) + "/" + filePath;
	}

	private String getDirPathFromDateTime(DateTime dateTime) {
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = year + File.separator + NumberUtils.get2CharNumber(month) + File.separator
				+ NumberUtils.get2CharNumber(day) + File.separator + NumberUtils.get2CharNumber(hour);
		return CommonConfig.get(PayTVConfig.RAW_LOG_DIR) + "/" + path;
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

	private void parseLog(DateTime dateTime) throws IOException {
		long start = System.currentTimeMillis();
		int countPrint = 0;
		int countTotal = 0;
		List<String> listLog = new ArrayList<>();
		String filePath = getFilePathFromDateTime(dateTime);
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = br.readLine();
		while (line != null) {
			if (!line.isEmpty()) {
				try {
					Source source = new Gson().fromJson(line, Source.class);
					String received_at_string = source.getReceived_at();
					DateTime received_at = PayTVUtils.parseReceived_at(received_at_string);
					Fields fields = source.getFields();
					String customerId = fields.getCustomerId();
					String contract = fields.getContract();
					String logId = fields.getLogId();
					String appName = fields.getAppName();
					String itemId = fields.getItemId();
					String realTimePlaying = fields.getRealTimePlaying();
					String sessionMainMenu = fields.getSessionMainMenu();
					String boxTime = fields.getBoxTime();
					String ip_wan = fields.getIp_wan();
					if (StringUtils.isNumeric(customerId) && logId != null && appName != null && received_at != null
							&& !setUserSpecial.contains(customerId)) {
						String writeLog = customerId + "," + contract + "," + logId + "," + appName + "," + itemId + ","
								+ realTimePlaying + "," + sessionMainMenu + "," + boxTime + "," + received_at_string
								+ "," + ip_wan;
						listLog.add(writeLog);
					}
				} catch (Exception e) {
					PayTVUtils.LOG_ERROR.error("Error parse json: " + line);
				}
			}
			line = br.readLine();
			countTotal++;
		}
		br.close();
		countPrint = printLogParsed(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(),
				dateTime.getHourOfDay(), listLog, countPrint);
		PayTVUtils.LOG_INFO.info("Done file: " + filePath + " | Print: " + countPrint + " | Total: " + countTotal
				+ " | Time: " + (System.currentTimeMillis() - start) + " | At: " + System.currentTimeMillis());
	}

	private int printLogParsed(int year, int month, int day, int hour, List<String> listLog, int count)
			throws IOException {
		String path = CommonConfig.get(PayTVConfig.PARSED_LOG_HDFS_DIR) + "/" + year;
		String monthString = NumberUtils.get2CharNumber(month);
		String dayString = NumberUtils.get2CharNumber(day);
		String hourString = NumberUtils.get2CharNumber(hour);
		path = path + "/" + monthString + "/" + dayString + "/" + hourString;
		if (!hdfsIO.isExist(path)) {
			hdfsIO.createFolder(path);
		}
		path = path + "/" + year + "-" + monthString + "-" + dayString + "_" + hourString + "_log_parsed.csv";
		if (listLog.size() > 0) {
			System.out.println("Push data to : " + path);
			BufferedWriter bw;
			if (hdfsIO.isExist(path)) {
				bw = hdfsIO.getWriteStreamAppend(path);
			} else {
				bw = hdfsIO.getWriteStreamNew(path);
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

	public static void hashCustomerId() throws IOException, NoSuchAlgorithmException {
		List<File> listFile = Arrays.asList(new File(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/t4").listFiles());
		FileUtils.sortListFileDateTime(listFile);
		String hashFolder = CommonConfig.get(PayTVConfig.MAIN_DIR) + "/t4_hash";
		FileUtils.createFolder(hashFolder);

		Map<String, String> mapHashCode = new HashMap<>();
		BufferedReader br_h = new BufferedReader(
				new FileReader(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/hash/hash_old.csv"));
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

		PrintWriter pr = new PrintWriter(new FileWriter(CommonConfig.get(PayTVConfig.MAIN_DIR) + "/Hash.csv"));
		for (String customerId : mapHashCode.keySet()) {
			pr.println(customerId + "," + mapHashCode.get(customerId));
		}
		pr.close();
	}

}
