package com.fpt.tv;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class CancelRecord {
	private String contract;
	private String mac;
	private String id;
	private Date startDate;
	private Date stopDate;
	private int tuan;
	private long dayUse;
	
	
	public CancelRecord(String record){
		String[] arr = record.split(",");
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm a");
		if (arr.length == 6) {
			contract = arr[0];
			mac = arr[1];
			id = arr[2];
			try {
				startDate = sdf.parse(arr[3]);
			} catch (ParseException e1) {
				System.out.println("Parse date error !");
				e1.printStackTrace();
			}
			try {
				stopDate = sdf.parse(arr[4]);
			} catch (ParseException e) {
				System.out.println("Parse date error !");
				e.printStackTrace();
			}
			tuan = Integer.parseInt(arr[5]);
			dayUse = TimeUnit.DAYS.convert(stopDate.getTime() - startDate.getTime(), TimeUnit.MILLISECONDS);
		}
	}

	public String getContract() {
		return contract;
	}

	public String getMAC() {
		return mac;
	}

	public String getId() {
		return id;
	}

	public Date getStartDate() {
		return startDate;
	}

	public Date getStopDate() {
		return stopDate;
	}

	public int getTuan() {
		return tuan;
	}
	
	public long getDayUse(){
		return dayUse;
	}
}
