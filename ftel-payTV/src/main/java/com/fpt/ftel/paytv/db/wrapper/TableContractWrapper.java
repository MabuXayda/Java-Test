package com.fpt.ftel.paytv.db.wrapper;

import org.joda.time.DateTime;

public class TableContractWrapper {
	String contract;
	Integer box_count;
	String payment_method;
	String point_set;
	String location;
	String region;
	Integer status_id;
	DateTime start_date;
	DateTime stop_date;
	String stop_reason;
	Double late_pay_score;
	
	public String getContract() {
		return contract;
	}

	public void setContract(String contract) {
		this.contract = contract;
	}

	public Integer getBox_count() {
		return box_count;
	}

	public void setBox_count(Integer box_count) {
		this.box_count = box_count;
	}

	public String getPayment_method() {
		return payment_method;
	}

	public void setPayment_method(String payment_method) {
		this.payment_method = payment_method;
	}

	public String getPoint_set() {
		return point_set;
	}

	public void setPoint_set(String point_set) {
		this.point_set = point_set;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public Integer getStatus_id() {
		return status_id;
	}

	public void setStatus_id(Integer status_id) {
		this.status_id = status_id;
	}

	public DateTime getStart_date() {
		return start_date;
	}

	public void setStart_date(DateTime start_date) {
		this.start_date = start_date;
	}

	public DateTime getStop_date() {
		return stop_date;
	}

	public void setStop_date(DateTime stop_date) {
		this.stop_date = stop_date;
	}

	public String getStop_reason() {
		return stop_reason;
	}

	public void setStop_reason(String stop_reason) {
		this.stop_reason = stop_reason;
	}

	public Double getLate_pay_score() {
		return late_pay_score;
	}

	public void setLate_pay_score(Double late_pay_score) {
		this.late_pay_score = late_pay_score;
	}
}
