package com.fpt.tv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

public class LogObjectOld1 {
	String customerId = "";
	String mac = "";
	String appId = "";
	String logId = "";
	String event = "";
	String itemId = "";
	String sessionMainMenu = "";
	String realTimePlaying = "";

	public LogObjectOld1(String line) {
		Record record = new Gson().fromJson(line, Record.class);
		this.customerId = record.getSource().getFields().getCustomerId();
		this.mac = record.getSource().getFields().getMac();
		this.appId = record.getSource().getFields().getAppId();

	}

	public static void main(String[] args) throws IOException {
//		BufferedReader br = new BufferedReader(new FileReader(DataPrepare.DIR + "test.txt"));
//		String line = br.readLine();
//		Record record = new Gson().fromJson(line, Record.class);
//		String customerId = record.getSource().getFields().getCustomerId();
//		String mac = record.getSource().getFields().getMac();
//		String appId = record.getSource().getFields().getAppId();
//		String logId = record.getSource().getFields().getLogId();
//		System.out.println(customerId + "," + mac + "," + appId + "," + logId);
		String x = "NaN";
		Double y = Double.parseDouble(x);
		System.out.println(y);
		if(y == Double.NaN){
			System.out.println("True");
		}
	}

}

//class Record {
//	// @SerializedName("_index")
//	// String index;
//	// @SerializedName("_type")
//	// String type;
//	// @SerializedName("_id")
//	// String id;
//	// @SerializedName("_score")
//	// Integer score;
//	@SerializedName("_source")
//	Source source;
//
//	public Source getSource() {
//		return source;
//	}
//}

//class Source {
//	// @SerializedName("tags")
//	// List<String> tags;
//	// @SerializedName("@timestamp")
//	// String timestamp;
//	// @SerializedName("index_day")
//	// String index_day;
//	// @SerializedName("host")
//	// String host;
//	// @SerializedName("type")
//	// String type;
//	// @SerializedName("received_at")
//	// String received_at;
//	// @SerializedName("path")
//	// String path;
//	// @SerializedName("message")
//	// String message;
//	// @SerializedName("@version")
//	// String version;
//	@SerializedName("fields")
//	Fields fields;
//
//	public Fields getFields() {
//		return fields;
//	}
//}

//class Fields {
//	// @SerializedName("Duration")
//	// Integer duration;
//	// @SerializedName("ItemName")
//	// String itemName;
//	// @SerializedName("Key")
//	// String key;
//	// @SerializedName("isLandingPage")
//	// String isLandingPage;
//	// @SerializedName("BoxTime")
//	// String boxTime;
//	// @SerializedName("ElapsedTimePlaying")
//	// String elapsedTimePlaying;
//	// @SerializedName("Screen")
//	// String screen;
//	// @SerializedName("SecondaryDNS")
//	// String secondaryDNS;
//	// @SerializedName("LocalType")
//	// String localType;
//	// @SerializedName("SubnetMask")
//	// String subnetMask;
//	// @SerializedName("ip_wan")
//	// String ip_wan;
//	// @SerializedName("PreviousSubMenuId")
//	// String previousSubMenuId;
//	// @SerializedName("PreviousItemId")
//	// String previousItemId;
//	// @SerializedName("Contract")
//	// String contract;
//	// @SerializedName("Multicast")
//	// String multicast;
//	// @SerializedName("PrimaryDNS")
//	// String primaryDNS;
//	// @SerializedName("DefaultGetway")
//	// String defaultGetway;
//	// @SerializedName("Ip")
//	// String ip;
//	// @SerializedName("Session")
//	// String session;
//	// @SerializedName("PreviousScreen")
//	// String previousScreen;
//	// @SerializedName("PreviousAppId")
//	// String previousAppId;
//	// @SerializedName("Firmware")
//	// String firmware;
//	// @SerializedName("PreviousDuration")
//	// String previousDuration;
//	// @SerializedName("AppName")
//	// String appName;
//	// @SerializedName("SessionSubMenu")
//	// String sessionSubMenu;
//	// @SerializedName("SessionMenu")
//	// String sessionMenu;
//	@SerializedName("RealTimePlaying")
//	String realTimePlaying;
//	@SerializedName("SessionMainMenu")
//	String sessionMainMenu;
//	@SerializedName("ItemId")
//	String itemId;
//	@SerializedName("Event")
//	String event;
//	@SerializedName("LogId")
//	String logId;
//	@SerializedName("AppId")
//	String appId;
//	@SerializedName("Mac")
//	String mac;
//	@SerializedName("CustomerID")
//	String customerId;
//
//	public String getLogId() {
//		return logId;
//	}
//
//	public String getEvent() {
//		return event;
//	}
//
//	public String getCustomerId() {
//		return customerId;
//	}
//
//	public String getItemId() {
//		return itemId;
//	}
//
//	public String getAppId() {
//		return appId;
//	}
//
//	public String getMac() {
//		return mac;
//	}
//
//	public String getSessionMainMenu() {
//		return sessionMainMenu;
//	}
//
//	public Double getRealTimePlayingValue() {
//		Double result = null;
//		try {
//			result = Double.parseDouble(realTimePlaying);
//		} catch (Exception e) {
//			// TODO: handle exception
//		}
//		if (result == null || result.isNaN()) {
//			return null;
//		} else {
//			return result;
//		}
//	}
//}
