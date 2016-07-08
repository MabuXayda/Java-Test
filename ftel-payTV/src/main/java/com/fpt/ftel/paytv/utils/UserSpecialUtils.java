package com.fpt.ftel.paytv.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;

public class UserSpecialUtils {

	public static List<String> getListUserSpecial() throws JsonIOException, JsonSyntaxException, FileNotFoundException {
		String filePath = "";
		UserSpecialApi api = new Gson().fromJson(new JsonReader(new FileReader(filePath)), UserSpecialApi.class);
		List<CustomerID> listCustomerId = api.getRoot().getListCustomer();
		List<String> result = new ArrayList<>();
		for (CustomerID id : listCustomerId) {
			result.add(id.getCustomerId());
		}
		return result;
	}

	public class UserSpecialApi {
		@SerializedName("Root")
		Root root;

		public Root getRoot() {
			return root;
		}
	}

	public class Root {
		@SerializedName("List_Customer")
		List<CustomerID> list_Customer;

		public List<CustomerID> getListCustomer() {
			return list_Customer;
		}
	}

	public class CustomerID {
		@SerializedName("CustomerID")
		String customerID;

		public String getCustomerId() {
			return customerID;
		}
	}

}
