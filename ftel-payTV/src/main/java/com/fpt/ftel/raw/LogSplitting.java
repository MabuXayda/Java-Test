package com.fpt.ftel.raw;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.FileUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class LogSplitting {
	// HdfsIO hdfsIO = new HdfsIO(host, port);

	public static void main(String[] args) throws IOException {
		LogSplitting logSplitting = new LogSplitting();
		logSplitting.process();
		System.out.println("DONE");
	}

	public void process() throws IOException {
		List<String> listFilePath = new ArrayList<>();
		FileUtils.loadListFilePath(listFilePath, new File(CommonConfig.getInstance().get(CommonConfig.PARSED_LOG_DIR) + "/t4"));
		FileUtils.sortListFilePathDateTime(listFilePath);
		splitFile(listFilePath, CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/hadoop_hdfs/2016");

	}

	public void splitFile(List<String> listFilePath, String outputDir) throws IOException {
		FileUtils.createFolder(outputDir);
		DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
		List<String> listLog = new ArrayList<>();
		LocalDate startDate = dtf.parseDateTime("2016-02-01").toLocalDate();
		Integer startMonth = 2;
		Integer startDay = 1;
		Integer startHour = 1;
		
		for (String filePath : listFilePath) {
			Integer countPrint = 0;
			Integer countTotal = 0;
			BufferedReader br = new BufferedReader(new FileReader(filePath));
			String line = br.readLine();
			while (line != null) {
				countTotal ++;
				String[] arr = line.split(",");
				String customerId = arr[0];
				DateTime received_at = PayTVUtils.parseReceived_at(arr[8]);
				if (received_at != null) {
					
					Integer month = received_at.getMonthOfYear();
					Integer day = received_at.getDayOfMonth();
					Integer hour = received_at.getHourOfDay();
//					if(Utils.isNumeric(customerId)){
//						LocalDate simpleDate = received_at.toLocalDate();
//						if(simpleDate.equals(startDate) && hour == startHour){
//							listLog.add(line);
//						}else {
//							String path = outputDir + "/" + startDate.toString();
//							FileUtils.createFolder(new File(path));
//							path = path + "/" + startHour;
//							FileUtils.createFolder(new File(path));
//							path = path + "/log_parsed.csv";
//							
//							PrintWriter pr = new PrintWriter(new FileWriter(path, true));
//							for (String log : listLog) {
//								pr.println(log);
//								countPrint ++;
//							}
//							pr.close();
//							
//							startDate = simpleDate;
//							startHour = hour;
//							listLog.clear();
//							listLog.add(line);
//						}
//					}
					
					if (StringUtils.isNumeric(customerId)) {
						if (month == startMonth && day == startDay && hour == startHour) {
							listLog.add(line);
						} else {
							String path = outputDir + "/" + String.format("%02d", startMonth);
							FileUtils.createFolder(path);
							path = path + "/" + String.format("%02d", startDay);
							FileUtils.createFolder(path);
							path = path + "/" + String.format("%02d", startHour);
							FileUtils.createFolder(path);
							path = path + "/2016-" + String.format("%02d", startMonth) + "-"
									+ String.format("%02d", startDay) + "_" + String.format("%02d", startHour)
									+ "_log_parsed.csv";
							
//							Integer index = 0;
//							String pathIndex = path + "/raw_" + index;
//							while (FileUtils.exitFile(new File(pathIndex))) {
//								index += 1;
//								pathIndex = path + "/raw_" + index;
//							}
							
							PrintWriter pr = new PrintWriter(new FileWriter(path, true));
							
							for (String log : listLog) {
								pr.println(log);
								countPrint ++;
							}
							pr.close();
							startMonth = month;
							startDay = day;
							startHour = hour;
							listLog = new ArrayList<>();
							listLog.add(line);
						}
					}
					
				}
				line = br.readLine();
			}
			br.close();
			System.out.println("Done file: " + filePath + " | CountPrint: " + countPrint + " | CountTotal: " + countTotal );
		}
	}

}
