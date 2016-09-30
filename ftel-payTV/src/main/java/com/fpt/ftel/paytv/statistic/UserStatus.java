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
	public static final String LOCATION = "LOCATION";
	public static final String STATUS_ID = "STATUS_ID";
	public static final String START_DATE = "START_DATE";
	public static final String STOP_DATE = "STOP_DATE";
	public static final String LAST_ACTIVE = "LAST_ACTIVE";
	
	

	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
		DateTime date = PayTVUtils.FORMAT_DATE_TIME.parseDateTime("2016-07-31 05:00:00");
		System.out.println(date.withTimeAtStartOfDay());

	}

	public static Map<String, DateTime> getMapUserDateCondition(String fileUserActive, String dateCondition,
			String fileUserChurn) throws IOException {
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
		Map<String, DateTime> mapUserActive = getMapUserActive(fileUserActive);
		for (String customerId : mapUserActive.keySet()) {
			mapUserDateCondition.put(customerId, PayTVUtils.FORMAT_DATE_TIME.parseDateTime(dateCondition));
		}

		Map<String, Map<String, DateTime>> mapUserChurn = getMapUserChurn(fileUserChurn);
		for (String customerId : mapUserChurn.keySet()) {
			mapUserDateCondition.put(customerId, mapUserChurn.get(customerId).get("StopDate"));
		}
		return mapUserDateCondition;
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
			Set<String> setUser = getSetUserCancelFromString(content);
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

	public static Map<String, DateTime> getMapUserActive(String filePath) throws IOException {
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

	public static Map<String, Map<String, DateTime>> getMapUserChurn(String filePath) throws IOException {
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
		UserTestApi api = new Gson().fromJson(new JsonReader(new FileReader(filePath)), UserTestApi.class);
		List<UserTestApi.Root.CustomerID> listCustomerId = api.getRoot().getListCustomer();
		Set<String> result = new HashSet<>();
		for (UserTestApi.Root.CustomerID id : listCustomerId) {
			result.add(id.getCustomerId());
		}
		return result;
	}

	public static Set<String> getSetUserCancelFromString(String str) {
		UserCancelApi api = new Gson().fromJson(str, UserCancelApi.class);
		List<UserCancelApi.Root.Item> listItem = api.getRoot().getListItem();
		Set<String> result = new HashSet<>();
		for (UserCancelApi.Root.Item item : listItem) {
			result.add(item.getCustomerId());
		}
		return result;
	}

//	public static Set<String> getSetUserCancelFromFile(String filePath)
//			throws JsonIOException, JsonSyntaxException, FileNotFoundException {
//		UserCancelApi api = new Gson().fromJson(new JsonReader(new FileReader(filePath)), UserCancelApi.class);
//		List<UserCancelApi.Root.Item> listItem = api.getRoot().getListItem();
//		Set<String> result = new HashSet<>();
//		for (UserCancelApi.Root.Item item : listItem) {
//			result.add(item.getCustomerId());
//		}
//		return result;
//	}
	
	public static Map<String, Map<String, String>> getSetUserChurnInfo(DateTime dateTime) throws IOException{
		Map<String, Map<String, String>> result = new HashMap<>();
		
		URL url = new URL(CommonConfig.get(PayTVConfig.GET_USER_CHURN_API) + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
		UserCancelApi apiCancel = new Gson().fromJson(IOUtils.toString(url, "UTF-8"), UserCancelApi.class);
		for(UserCancelApi.Root.Item item : apiCancel.getRoot().getListItem()){
			Map<String, String> newInfo = new HashMap<>();
			newInfo.put(STATUS_ID, item.getStatus());
			newInfo.put(STOP_DATE, PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
			result.put(item.getCustomerId(), newInfo);
		}
		
		url = new URL(CommonConfig.get(PayTVConfig.GET_USER_REGISTER_API) + PayTVUtils.FORMAT_DATE_TIME_SIMPLE.print(dateTime));
		UserRegisterApi apiRegister = new Gson().fromJson(IOUtils.toString(url, "UTF-8"), UserRegisterApi.class);
		for(UserRegisterApi.Root.Item item : apiRegister.getRoot().getListItem()){
			Map<String, String> newInfo = new HashMap<>();
			newInfo.put(CONTRACT, item.getContract());
			newInfo.put(STATUS_ID, "1");
			newInfo.put(LOCATION, item.getLocation());
			result.put(item.getCustomerId(), newInfo);
		}
		
		return result;
	}

}
