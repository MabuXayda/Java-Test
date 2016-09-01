package com.fpt.ftel.paytv.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.config.PayTVConfig;
import com.fpt.ftel.core.utils.FileUtils;

public class ServiceUtils {
	public static final String PARSE_LOG_SERVICE_MISSING = "/ParseLogService_missing";
	public static final String TABLE_NOW_SERVICE_MISSING = "/TableNowService_missing";
	public static final String TABLE_DAILY_SERVICE_MISSING = "/TableDailyService_missing";
	public static final String TABLE_PROFILE_SERVICE_MISSING = "/TableProfileService_missing";
	
	public static List<String> getListProcessMissing(String file) throws IOException {
		List<String> listMissing = new ArrayList<>();
		if (FileUtils.isExistFile(CommonConfig.get(PayTVConfig.SERVICE_MONITOR_DIR) + file)) {
			BufferedReader br = new BufferedReader(
					new FileReader(CommonConfig.get(PayTVConfig.SERVICE_MONITOR_DIR) + file));
			String line = br.readLine();
			while (line != null) {
				listMissing.add(line);
				line = br.readLine();
			}
			br.close();
		}
		return listMissing;
	}

	public static void printListProcessMissing(List<String> listMissing, String file) throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(CommonConfig.get(PayTVConfig.SERVICE_MONITOR_DIR) + file));
		if (listMissing.size() == 0) {
			pr.print("");
		} else {
			for (String fileMissing : listMissing) {
				pr.println(fileMissing);
			}
		}
		pr.close();
	}

	public static List<DateTime> getListDateProcessMissing(String file) throws IOException {
		List<DateTime> listMissing = new ArrayList<>();
		if (FileUtils.isExistFile(CommonConfig.get(PayTVConfig.SERVICE_MONITOR_DIR) + file)) {
			BufferedReader br = new BufferedReader(
					new FileReader(CommonConfig.get(PayTVConfig.SERVICE_MONITOR_DIR) + file));
			String line = br.readLine();
			while (line != null) {
				listMissing.add(PayTVUtils.FORMAT_DATE_TIME.parseDateTime(line));
				line = br.readLine();
			}
			br.close();
		}
		return listMissing;
	}
}
