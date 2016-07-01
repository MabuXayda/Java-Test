package com.fpt.tv.object.raw;

import com.google.gson.annotations.SerializedName;

public class Source {
	// @SerializedName("tags")
	// List<String> tags;
	// @SerializedName("@timestamp")
	// String timestamp;
	// @SerializedName("index_day")
	// String index_day;
	// @SerializedName("host")
	// String host;
	// @SerializedName("type")
	// String type;
	// @SerializedName("path")
	// String path;
	// @SerializedName("message")
	// String message;
	// @SerializedName("@version")
	// String version;

	@SerializedName("received_at")
	String received_at;

	public String getReceived_at() {
		return received_at;
	}

	@SerializedName("fields")
	Fields fields;

	public Fields getFields() {
		return fields;
	}
}
