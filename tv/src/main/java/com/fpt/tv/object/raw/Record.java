package com.fpt.tv.object.raw;

import com.google.gson.annotations.SerializedName;

public class Record {
	// @SerializedName("_index")
	// String index;
	// @SerializedName("_type")
	// String type;
	// @SerializedName("_id")
	// String id;
	// @SerializedName("_score")
	// Integer score;

	@SerializedName("_source")
	Source source;

	public Source getSource() {
		return source;
	}
}
