package com.fpt.ftel.paytv.statistic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.DateTimeUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.object.UserCancelApi;
import com.fpt.ftel.paytv.object.UserRegisterApi;
import com.fpt.ftel.paytv.object.UserTestApi;
import com.fpt.ftel.paytv.utils.PayTVConfig;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

public class UserStatus {
	public static final String CONTRACT = "CONTRACT";
	public static final String CUSTOMER_ID = "CUSTOMER_ID";
	public static final String SERVICE_ID = "SERVICE_ID";
	public static final String SERVICE_NAME = "SERVICE_NAME";
	public static final String LOCATION_ID = "LOCATION_ID";
	public static final String LOCATION = "LOCATION";
	public static final String REGION = "REGION";
	public static final String STATUS_ID = "STATUS_ID";
	public static final String START_DATE = "START_DATE";
	public static final String STOP_DATE = "STOP_DATE";
	public static final String LAST_ACTIVE = "LAST_ACTIVE";

	private static String status;

	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
		Set<String> setUser = getSetUserSpecial("/home/tunn/data/tv/data_support/zz");
		for (String id : setUser) {
			System.out.println(id);
		}
		System.out.println("DONE");
	}

	public static Map<String, DateTime> getMapUserDateCondition(String fileTotalUser, DateTime dateCondition)
			throws IOException {
		Map<String, DateTime> result = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(fileTotalUser));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			String customerId = arr[1];
			int lifeTime = Integer.parseInt(arr[5]);
			if (lifeTime >= 28) {
				boolean churn = new Boolean(arr[6]);
				DateTime contDate;
				if (churn == true) {
					contDate = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[4]);
				} else {
					contDate = dateCondition;
				}
				result.put(customerId, contDate);
			}
			line = br.readLine();
		}
		br.close();
		return result;
	}

	public static Map<String, DateTime> getMapUserActiveDateCondition(String fileUserActive, DateTime dateCondition)
			throws IOException {
		Map<String, DateTime> mapUserActive = getMapUserActive(fileUserActive);
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		for (String customerId : mapUserActive.keySet()) {
			mapUserDateCondition.put(customerId, dateCondition);
		}
		return mapUserDateCondition;
	}

	public static Map<String, DateTime> getMapUserChurnDateCondition(DateTime dateTime) throws IOException {
		LocalDate localDate = dateTime.toLocalDate();
		Map<String, DateTime> result = new HashMap<>();
		List<String> listDate = DateTimeUtils.getListDateInMonth(localDate);
		for (String dateString : listDate) {
			URL url = new URL(CommonConfig.get(PayTVConfig.GET_USER_CHURN_API) + dateString);
			String content = IOUtils.toString(url, "UTF-8");
			Set<String> setUser = getSetUserChurnApi(content);
			for (String user : setUser) {
				result.put(user, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.parseDateTime(dateString));
			}
		}
		return result;
	}

	public static Map<String, DateTime> getMapUserChurnDateCondition(String fileUserChurn) throws IOException {
		Map<String, Map<String, DateTime>> mapUserChurn = getMapUserChurn(fileUserChurn);
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		for (String customerId : mapUserChurn.keySet()) {
			mapUserDateCondition.put(customerId, mapUserChurn.get(customerId).get("StopDate"));
		}
		return mapUserDateCondition;
	}

	private static Map<String, DateTime> getMapUserActive(String filePath) throws IOException {
		Map<String, DateTime> mapUserActive = new HashMap<>();
		int count = 0;
		File file = new File(filePath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 5) {
				if (StringUtils.isNumeric(arr[0])) {
					DateTime startDate = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[2]);
					if (startDate != null) {
						mapUserActive.put(arr[0], startDate);
						count++;
					}
				}
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Load " + file.getName() + " : " + count);
		return mapUserActive;
	}

	private static Map<String, Map<String, DateTime>> getMapUserChurn(String filePath) throws IOException {
		Map<String, Map<String, DateTime>> mapUserChurn = new HashMap<>();
		int count = 0;
		File file = new File(filePath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 6) {
				if (StringUtils.isNumeric(arr[0])) {
					DateTime startDate = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[2]);
					DateTime stopDate = PayTVUtils.FORMAT_DATE_TIME.parseDateTime(arr[4]);
					if (startDate != null && stopDate != null) {
						Map<String, DateTime> mapDate = new HashMap<>();
						mapDate.put("StartDate", startDate);
						mapDate.put("StopDate", stopDate);
						mapUserChurn.put(arr[0], mapDate);
						count++;
					}
				}
			}
			line = br.readLine();
		}
		br.close();
		System.out.println("Load " + file.getName() + " : " + count);
		return mapUserChurn;
	}

	public static Set<String> getSetUserSpecial(String filePath)
			throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		Set<String> result = new HashSet<>();
		UserTestApi api = new Gson().fromJson(new JsonReader(new FileReader(filePath)), UserTestApi.class);
		List<UserTestApi.Root.CustomerID> listCustomerId = api.getRoot().getListCustomer();
		if (listCustomerId != null) {
			for (UserTestApi.Root.CustomerID id : listCustomerId) {
				result.add(id.getCustomerId());
			}
		} else {
			status = "ERROR LOAD API USER CANCEL";
			System.out.println(status);
			PayTVUtils.LOG_ERROR.error(status);
			PayTVUtils.LOG_INFO.info(status);
		}
		return result;
	}

	public static Set<String> getSetUserChurnApi(String content) {
		UserCancelApi api = new Gson().fromJson(content, UserCancelApi.class);
		List<UserCancelApi.Root.Item> listItem = api.getRoot().getListItem();
		Set<String> result = new HashSet<>();
		for (UserCancelApi.Root.Item item : listItem) {
			result.add(item.getCustomerId());
		}
		return result;
	}

	public static Map<String, Map<String, String>> getMapUserChurnApi(String content, DateTime dateTime) {
		Map<String, Map<String, String>> result = new HashMap<>();
		UserCancelApi apiCancel = new Gson().fromJson(content, UserCancelApi.class);
		for (UserCancelApi.Root.Item item : apiCancel.getRoot().getListItem()) {
			Map<String, String> newInfo = new HashMap<>();
			newInfo.put(STATUS_ID, item.getStatus());
			newInfo.put(STOP_DATE, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
			result.put(item.getCustomerId(), newInfo);
		}
		return result;
	}

	public static Map<String, Map<String, String>> getMapUserRegisterApi(String content, DateTime dateTime)
			throws IOException {
		Map<String, Map<String, String>> result = new HashMap<>();
		Map<String, Map<String, String>> mapLocation = getMapLocation();
		UserRegisterApi apiRegister = new Gson().fromJson(content, UserRegisterApi.class);
		for (UserRegisterApi.Root.Item item : apiRegister.getRoot().getListItem()) {
			Map<String, String> newInfo = new HashMap<>();
			newInfo.put(CONTRACT, item.getContract());
			newInfo.put(STATUS_ID, "1");
			newInfo.put(LOCATION, item.getLocation());
			String location_code = item.getContract().substring(0, 2);
			newInfo.put(LOCATION_ID, mapLocation.get(location_code).get(LOCATION_ID));
			newInfo.put(LOCATION, mapLocation.get(location_code).get(LOCATION));
			newInfo.put(REGION, mapLocation.get(location_code).get(REGION));
			newInfo.put(START_DATE, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
			result.put(item.getCustomerId(), newInfo);
		}
		return result;
	}

	public static Map<String, Map<String, String>> getMapLocation() throws IOException {
		Map<String, Map<String, String>> result = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(CommonConfig.get(PayTVConfig.LOCATION_MAPPING)));
		String line = br.readLine();
		line = br.readLine();
		while (line != null) {
			Map<String, String> mapInfo = new HashMap<>();
			String[] arr = line.split(",");
			mapInfo.put(LOCATION_ID, arr[0]);
			mapInfo.put(LOCATION, arr[2]);
			mapInfo.put(REGION, arr[3]);
			result.put(arr[1], mapInfo);
			line = br.readLine();
		}
		br.close();
		return result;
	}

}
