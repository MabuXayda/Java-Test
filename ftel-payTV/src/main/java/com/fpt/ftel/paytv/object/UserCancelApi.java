package com.fpt.ftel.paytv.object;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class UserCancelApi {
	public class Root {
		public class Item {
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
