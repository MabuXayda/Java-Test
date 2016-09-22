package com.fpt.ftel.paytv.object;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class UserTestApi {
	public class Root {
		public class CustomerID {
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
