package com.fpt.ftel.paytv.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.NumberUtils;
import com.fpt.ftel.hdfs.HdfsIOSimple;
import com.fpt.ftel.paytv.db.PostgreSQLPayTV;
import com.fpt.ftel.paytv.statistic.UserUsage;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class TableNowService {
	private UserUsage userUsage;
	private HdfsIOSimple hdfsIOSimple;
	private PostgreSQLPayTV postgreSQLPayTV;

	public void process(String dateString) throws IOException {
		hdfsIOSimple = new HdfsIOSimple();
		DateTime dateTime = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateString);
		String filePath = getProcessFilePath(dateTime);
		if (hdfsIOSimple.isExist(filePath)) {
			filePath = getProcessFilePath(dateTime.minusHours(1));
		}
	}

	private void printListParseMissing(List<String> listMissing) throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(PARSE_MISSING));
		if (listMissing.size() == 0) {
			pr.print("");
		} else {
			for (String fileMissing : listMissing) {
				pr.println(fileMissing);
			}
		}
		pr.close();
	}

	public String getProcessFilePath(DateTime dateTime) {
		int year = dateTime.getYearOfEra();
		int month = dateTime.getMonthOfYear();
		int day = dateTime.getDayOfMonth();
		int hour = dateTime.getHourOfDay();
		String path = CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_HDFS_DIR) + "/" + year + "/"
				+ String.format("%02d", month) + "/" + String.format("%02d", day) + "/" + String.format("%02d", hour);
		path = path + "/2016-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "_"
				+ String.format("%02d", hour) + "_log_parsed.csv";
		return path;
	}

	public void updateUserUsage(String filePath, DateTime dateTime) {
		userUsage = new UserUsage();
		postgreSQLPayTV = new PostgreSQLPayTV();
		Set<String> setUser = userUsage.calculateUserUsageHourly(filePath, hdfsIOSimple);
		Map<String, Map<Integer, Integer>> mapUserVectorHourly = userUsage.getMapUserVectorHourly();
		Map<String, Map<String, Integer>> mapUserVectorApp = userUsage.getMapUserVectorApp();
		Map<String, Map<String, Integer>> mapUserInfo = new HashMap<>();
		for (String customerId : setUser) {
			Map<String, Integer> mapInfo = new HashMap<>();
			for (Integer hour : mapUserVectorHourly.get(customerId).keySet()) {
				mapInfo.put(Integer.toString(hour), mapUserVectorHourly.get(customerId).get(hour));
			}
			mapInfo.putAll(mapUserVectorApp.get(customerId));
			mapUserInfo.put(customerId, mapInfo);
		}
		List<Set<String>> listSetUser = ListUtils.splitSetToSmallerSet(setUser, 500);
		for (Set<String> subSetUser : listSetUser) {
			Connection connection = postgreSQLPayTV.getConnection("localhost", 5432, "pay_tv", "tunn", "123456");
			Map<String, Map<String, Integer>> mapUserInfoUpdate = postgreSQLPayTV.queryUserInfo(connection, "now",
					PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime), subSetUser);
			for(String customerId: mapUserInfoUpdate.keySet()){
				Map<String, Integer> mapInfo = plus2Map(mapUserInfo.get(customerId), mapUserInfoUpdate.get(customerId));
				mapUserInfoUpdate.put(customerId, mapInfo);
			}
			Map<String, Map<String, Integer>> mapUserInfoInsert = new HashMap<>();
			for(String customerId : subSetUser){
				if(!mapUserInfoUpdate.containsKey(customerId)){
					mapUserInfoInsert.put(customerId, mapUserInfo.get(customerId));
				}
			}
		}

	}
	
	private Map<String, Integer> plus2Map(Map<String, Integer> map1, Map<String, Integer> map2){
		Map<String, Integer> mapResult = new HashMap<>();
		for(String key : map1.keySet()){
			mapResult.put(key, map1.get(key) + map2.get(key));
		}
		return mapResult;
	}
}
