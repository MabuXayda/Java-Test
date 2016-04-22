package com.fpt.tv;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LogObject {
	private String customerId;
	private String contract;
	private String logId;
	private String appName;
	private String itemId;
	private Double realTimePlaying;
	private DateTime sessionMainMenu;
	private DateTime received_at;

	public LogObject(String line) {
		String[] arr = line.split(",");
		this.customerId = arr[0];
		this.contract = arr[1];
		this.logId = arr[2];
		this.appName = arr[3];
		this.itemId = arr[4];
		this.realTimePlaying = parseRealTimePlaying(arr[5]);
		this.sessionMainMenu = parseSessionMainMenu(arr[6]);
		this.received_at = parseReceived_at(arr[7]);
	}

	public String getCustomerId() {
		return customerId;
	}

	public String getContract() {
		return contract;
	}

	public String getLogId() {
		return logId;
	}

	public String getAppName() {
		return appName;
	}

	public String getItemId() {
		return itemId;
	}

	public Double getRealTimePlaying() {
		return realTimePlaying;
	}

	public DateTime getSessionMainMenu() {
		return sessionMainMenu;
	}

	public DateTime getReceived_at() {
		return received_at;
	}

	private Double parseRealTimePlaying(String realTimePlaying) {
		Double time = null;
		try {
			time = Double.parseDouble(realTimePlaying);
		} catch (Exception e) {
			// TODO: handle exception
		}
		if (time == null || time.isNaN()) {
			time = null;
		}
		return time;
	}

	private DateTime parseSessionMainMenu(String sessionMainMenu) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		if (sessionMainMenu.length() != 36) {
			return null;
		} else {
			DateTime dt = dtf.parseDateTime(sessionMainMenu.substring(13, 32));
			return dt;
		}
	}

	private DateTime parseReceived_at(String received_at) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
		if (received_at.length() != 25) {
			return null;
		} else {
			DateTime dt = dtf.parseDateTime(received_at.substring(0, 19));
			return dt;
		}
	}

}
