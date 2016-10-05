package com.fpt.ftel.paytv.object;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ParsedLog {
	String customerId;
	String contract;
	String logId;
	String appName;
	String itemId;
	String realTimePlaying;
	String sessionMainMenu;
	String boxTime;
	String received_at;
	String ip_wan;

	public ParsedLog(String line) {
		String[] arr = line.split(",");
		this.customerId = arr[0];
		this.contract = arr[1];
		this.logId = arr[2];
		this.appName = arr[3];
		this.itemId = arr[4];
		this.realTimePlaying = arr[5];
		this.sessionMainMenu = arr[6];
		this.boxTime = arr[7];
		this.received_at = arr[8];
		this.ip_wan = arr[9];
	}

	public String getLine() {
		return customerId + "," + contract + "," + logId + "," + appName + "," + itemId + "," + realTimePlaying + ","
				+ sessionMainMenu + "," + boxTime + "," + received_at + "," + ip_wan;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getContract() {
		return contract;
	}

	public void setContract(String contract) {
		this.contract = contract;
	}

	public String getLogId() {
		return logId;
	}

	public void setLogId(String logId) {
		this.logId = logId;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getRealTimePlaying() {
		return realTimePlaying;
	}

	public Double getRealTimePlayingValue() {
		Double time = null;
		try {
			time = Double.parseDouble(realTimePlaying);
			if (time.isNaN()) {
				time = null;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return time;
	}

	public void setRealTimePlaying(String realTimePlaying) {
		this.realTimePlaying = realTimePlaying;
	}

	public void setRealTimePlaying(Double realTimePlaying) {
		this.realTimePlaying = Double.toString(realTimePlaying);
	}

	public String getSessionMainMenu() {
		return sessionMainMenu;
	}

	public DateTime getSessionMainMenuValue() {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		if (sessionMainMenu.length() != 36) {
			return null;
		} else {
			DateTime dt = null;
			try {
				dt = dtf.parseDateTime(sessionMainMenu.substring(13, 32));
			} catch (Exception e) {
				// TODO: handle exception
			}
			return dt;
		}
	}

	public void setSessionMainMenu(String sessionMainMenu) {
		this.sessionMainMenu = sessionMainMenu;
	}

	public String getBoxTime() {
		return boxTime;
	}

	public void setBoxTime(String boxTime) {
		this.boxTime = boxTime;
	}

	public String getReceived_at() {
		return received_at;
	}

	public DateTime getReceived_atValue() {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
		if (received_at.length() != 25) {
			return null;
		} else {
			DateTime dt = null;
			try {
				dt = dtf.parseDateTime(received_at.substring(0, 19));
			} catch (Exception e) {
				// TODO: handle exception
			}
			return dt;
		}
	}

	public void setReceived_at(String received_at) {
		this.received_at = received_at;
	}

	public void setReceived_at(DateTime dateTime) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
		this.received_at = dtf.print(dateTime) + "+07:00";
	}

	public String getIp_wan() {
		return ip_wan;
	}

	public void setIp_wan(String ip_wan) {
		this.ip_wan = ip_wan;
	}

	public static DateTime parseBoxTime(String boxTime) {
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy:MM:dd:HH:mm:ss");
		if (boxTime.length() != 23) {
			return null;
		} else {
			DateTime dt = null;
			try {
				dt = dtf.parseDateTime(boxTime.substring(0, 19));

			} catch (Exception e) {
				// TODO: handle exception
			}
			return dt;
		}
	}

}
