package com.fpt.dmp.prediction.age;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;

import com.fpt.fo.core.db.hbase.HBaseConnection;
import com.fpt.fo.core.db.hbase.HBaseUtils;
import com.fpt.fo.core.utils.conf.CommonConfiguration;

public class GetData {

	private static HBaseConnection hBaseConnection;

	public GetData() throws IOException {
		hBaseConnection = new HBaseConnection(
				CommonConfiguration.getInstance().get(CommonConfiguration.ZOOKEEPER_HOST_USER_ANALYTIC),
				CommonConfiguration.getInstance().get(CommonConfiguration.ZOOKEEPER_PORT_USER_ANALYTIC),
				HBaseConnection.TABLE_USER_ANALYTIC);
	}

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		GetData getData = new GetData();
		getData.getData();
		System.out.println("DONE");
	}

	public void getData() throws IOException {
		int hourly = 0;
		int topicSum = 0;
		int daily = 0;
		PrintWriter prHourly = new PrintWriter(new FileWriter(DataUtils.ID_HOURLY));
		PrintWriter prTopicSum = new PrintWriter(new FileWriter(DataUtils.ID_TOPIC));
		PrintWriter prDaily = new PrintWriter(new FileWriter(DataUtils.ID_DAILY));
		Map<String, Integer> mapIdAge = PrepareData.readMapIdAge(DataUtils.ID_AGE);
		DataUtils.printLabel(prHourly, 24);
		DataUtils.printLabel(prDaily, 7);
		DataUtils.printLabel(prTopicSum, 91);
		for (String id : mapIdAge.keySet()) {
			Result result = hBaseConnection.queryUserData(id);

			Map<Integer, Double> mapHourly = HBaseUtils.getHourly(result);
			List<Double> listHourly = new ArrayList<>();
			for (int i = 0; i < 24; i++) {
				listHourly.add(mapHourly.get(i));
			}
			if (DataUtils.sum(listHourly) > 0) {
				prHourly.print(id);
				for (int i = 0; i < listHourly.size(); i++) {
					prHourly.print("," + listHourly.get(i));
				}
				prHourly.println();
				hourly++;
			}

			Map<Integer, Double> mapDaily = HBaseUtils.getDaily(result);
			List<Double> listDaily = new ArrayList<>();
			for (int i = 0; i < 7; i++) {
				listDaily.add(mapDaily.get(i));
			}
			if (DataUtils.sum(listDaily) > 0) {
				prDaily.print(id);
				for (int i = 0; i < listDaily.size(); i++) {
					prDaily.print("," + listDaily.get(i));
				}
				prDaily.println();
				daily++;
			}

			Map<Integer, Double> mapTopicSum = HBaseUtils.getMapTopicSum(result);
			List<Double> listTopicSum = new ArrayList<>();
			for (int i = 0; i < 91; i++) {
				listTopicSum.add(mapTopicSum.get(i));
			}
			if (DataUtils.sum(listTopicSum) > 0) {
				prTopicSum.print(id);
				for (int i = 0; i < listTopicSum.size(); i++) {
					prTopicSum.print("," + listTopicSum.get(i));
				}
				prTopicSum.println();
				topicSum++;
			}

		}
		System.out.println("Hourly:" + hourly + " |TopicSum:" + topicSum + " |Daily:" + daily);
		prHourly.close();
		prDaily.close();
		prTopicSum.close();
	}

	public void cleanData() throws IOException {
		int countId = 0;
		int countAge = 0;
		BufferedReader br = new BufferedReader(new FileReader(DataUtils.ID_BIRTH));
		PrintWriter pr = new PrintWriter(new FileWriter(DataUtils.ID_AGE));
		String line = br.readLine();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		while (line != null) {
			if (line.split(",").length == 2) {
				String fosp_aid = line.split(",")[0];
				if (fosp_aid.length() == 16 || fosp_aid.length() == 32) {
					countId++;
					String dateString = line.split(",")[1];
					if (dateString.contains("/")) {
						Date date = null;
						try {
							date = dateFormat.parse(dateString);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						if (date != null) {
							Calendar cal = Calendar.getInstance();
							cal.setTime(date);
							int year = cal.get(Calendar.YEAR);
							int age = DataUtils.getAgeFromYear(year);
							if (age >= 10 && age <= 80) {
								countAge++;
								pr.println(fosp_aid + "," + age);
							}
						}
					}
				}
			}
			line = br.readLine();
		}
		System.out.println(countId + " | " + countAge);
		br.close();
		pr.close();
	}

}
