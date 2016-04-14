package com.fpt.tv;

import com.google.gson.annotations.SerializedName;

public class LogObject {

}


class Record {
	@SerializedName("_source")
	Source source;

	public Source getSource() {
		return source;
	}
}

class Source {
	@SerializedName("AppName")
	String appName;
	@SerializedName("Mac")
	String mac;
	@SerializedName("Date")
	String date;
	@SerializedName("TotalDuration")
	Integer totalDuration;
	
	public String getAppName(){
		return appName;
	}
	
	public String getMac(){
		return mac;
	}
	
	public String getDate(){
		return date;
	}
	
	public Integer getTotalDuration(){
		return totalDuration;
	}
	
}