package com.fpt.ftel.paytv.statistic;

import java.awt.Robot;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;

public class UserStatus {

	public static void main(String[] args) {
		try {
			Set<String> setId = UserStatus.getSetUserSpecial("/home/tunn/data/tv/support_data/paytv_get_dm_nv_vip");
			for (String id : setId) {
				System.out.println(id);
			}
		} catch (JsonIOException | JsonSyntaxException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	public static Map<String, DateTime> getMapUserChurnDateCondition(String fileUserChurn) throws IOException {
		Map<String, Map<String, DateTime>> mapUserChurn = getMapUserChurn(fileUserChurn);
		Map<String, DateTime> mapUserDateCondition = new HashMap<>();
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
		UserSpecialApi api = new Gson().fromJson(new JsonReader(new FileReader(filePath)), UserSpecialApi.class);
		List<UserSpecialApi.Root.CustomerID> listCustomerId = api.getRoot().getListCustomer();
		Set<String> result = new HashSet<>();
		for (UserSpecialApi.Root.CustomerID id : listCustomerId) {
			result.add(id.getCustomerId());
		}
		return result;
	}

	class UserSpecialApi {
		class Root {
			class CustomerID {
				@SerializedName("CustomerID")
				String customerID;

				public String getCustomerId() {
					return customerID;
				}
			}

			@SerializedName("List_Customer")
			List<CustomerID> list_Customer;

			public List<CustomerID> getListCustomer() {
				return list_Customer;
			}
		}

		@SerializedName("Root")
		Root root;

		public Root getRoot() {
			return root;
		}
	}
	
	public static 

	class UserRegis {
		class Root {
			class Item {
				@SerializedName("Contract")
				String contract;

				public String getContract() {
					return contract;
				}

				@SerializedName("CustomerID")
				String customerID;

				public String getCustomerId() {
					return customerID;
				}

				@SerializedName("ServiceID")
				String serviceID;

				public String getServiceId() {
					return serviceID;
				}
				// @SerializedName("ServiceName")
				// String serviceName;
				// @SerializedName("Location")
				// String location;
			}

			@SerializedName("item")
			List<Item> list_item;

			public List<Item> getListItem() {
				return list_item;
			}
		}

		@SerializedName("Root")
		Root root;

		public Root getRoot() {
			return root;
		}
	}

	class UserCancel{
		class Root {
			class Item {
				@SerializedName("Contract")
				String contract;

				public String getContract() {
					return contract;
				}

				@SerializedName("CustomerID")
				String customerID;

				public String getCustomerId() {
					return customerID;
				}

				@SerializedName("Status")
				String status;

				public String getStatus() {
					return status;
				}
			}

			@SerializedName("item")
			List<Item> list_item;

			public List<Item> getListItem() {
				return list_item;
			}
		}

		@SerializedName("Root")
		Root root;

		public Root getRoot() {
			return root;
		}
	}
	
}
