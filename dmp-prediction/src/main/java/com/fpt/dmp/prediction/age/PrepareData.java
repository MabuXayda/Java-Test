package com.fpt.dmp.prediction.age;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrepareData {

	public static final String HOURLY_SCALE = "data/daily_scale_filter.csv";
	public static final String[] AGE_RANGE = { "18-", "19-25", "26-35", "36-45", "46+" };

	private static Map<String, Integer> mapIdAge;
	private static Map<String, List<Double>> mapIdTopicSum;

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		init();
		System.out.println("DONE");
	}

	public static void init() throws IOException {
		// countIdByAge();
		// countIdByEtc(DataUtils.ID_HOURLY, "data/hourly_count_del.csv");
		// countIdByEtc(DataUtils.ID_DAILY, "data/daily_count.csv");
//		getTrainData();
//		 splitTrainTest();
		// clusterData();
//		 balanceData();
		featureChecker();
	}

	public static void clusterData() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DataUtils.AGER_HOURLY));
		PrintWriter pr = new PrintWriter(new FileWriter("data/hourlyCluster.csv"));
		Map<String, Integer> mapCountJoin = new HashMap<>();
		Map<String, Integer> mapConJoin = new HashMap<>();
		Map<String, List<Double>> mapTemp = new HashMap<>();
		mapConJoin.put(AGE_RANGE[0], 2);
		mapConJoin.put(AGE_RANGE[1], 9);
		mapConJoin.put(AGE_RANGE[2], 31);
		mapConJoin.put(AGE_RANGE[3], 13);
		mapConJoin.put(AGE_RANGE[4], 6);
		List<Double> zeroList = new ArrayList<>();
		for (int i = 0; i < 24; i++) {
			zeroList.add(0.0);
		}
		for (int i = 0; i < AGE_RANGE.length; i++) {
			mapCountJoin.put(AGE_RANGE[i], 0);
			mapTemp.put(AGE_RANGE[i], zeroList);
		}

		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			String age = arr[0];
			List<Double> list = new ArrayList<>();
			for (int i = 1; i <= 24; i++) {
				list.add(Double.parseDouble(arr[i]));
			}
			if (age.equals("age")) {
				pr.println(line);
			} else {
				int con = mapConJoin.get(age);
				int count = mapCountJoin.get(age);
				if (count < con - 1) {
					List<Double> oldList = mapTemp.get(age);
					List<Double> newList = DataUtils.plus2List(list, oldList);
					mapCountJoin.put(age, count + 1);
					mapTemp.put(age, newList);
				} else if (count == con - 1) {
					List<Double> oldList = mapTemp.get(age);
					List<Double> newList = DataUtils.plus2List(list, oldList);
					List<Double> finalList = DataUtils.scaleList(newList);
					pr.print(age);
					for (int i = 0; i < 24; i++) {
						pr.print("," + DataUtils.round(finalList.get(i)));
					}
					pr.println();
					mapCountJoin.put(age, 0);
					mapTemp.put(age, zeroList);
				}
			}
			line = br.readLine();
		}
		br.close();
		pr.close();
	}

	public static void featureChecker() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DataUtils.F_AGER_HOURLY));
		PrintWriter pr = new PrintWriter(new FileWriter("data/f_hourly_checker.csv"));
		String line = br.readLine();
		Map<String, List<Double>> mapCheck = new HashMap<>();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr[0].equals("age")) {
				line = br.readLine();
			} else if (arr.length == 11) {
				List<Double> listValue = new ArrayList<>();
				for (int i = 1; i < arr.length; i++) {
					listValue.add(Double.parseDouble(arr[i]));
				}
				if(mapCheck.containsKey(arr[0])){
					mapCheck.put(arr[0], DataUtils.plus2List(listValue, mapCheck.get(arr[0])));
				}else {
					mapCheck.put(arr[0], listValue);
				}
				line = br.readLine();
			}
		}
		DataUtils.printFilterLabel(pr);
		for(String age : mapCheck.keySet()){
			pr.print(age);
			List<Double> listValue = DataUtils.scaleList(mapCheck.get(age));
			for(int i = 0; i<listValue.size();i++){
				pr.print("," + DataUtils.round(listValue.get(i)));
			}
			pr.println();
		}
		br.close();
		pr.close();
	}

	public static void balanceData() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("data/train_smote.csv"));
		PrintWriter pr = new PrintWriter(new FileWriter("data/train_smote_balance.csv"));
		Map<String, Integer> mapAgeRange = new HashMap<>();
		for (String ageRange : AGE_RANGE) {
			mapAgeRange.put(ageRange, 0);
		}
		String line = br.readLine();
		while (line != null) {
			String age = line.split(",")[0];
			if (age.equals("age")) {
				pr.println(line);
			} else {
				int count = mapAgeRange.get(age);
				if (count < 5000) {
					pr.println(line);
					mapAgeRange.put(age, count + 1);
				}
			}
			line = br.readLine();
		}
		br.close();
		pr.close();
	}

	public static void splitTrainTest() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DataUtils.J_AGER_HOURLY));
		PrintWriter prTrain = new PrintWriter(new FileWriter("data/train.csv"));
		PrintWriter prTest = new PrintWriter(new FileWriter("data/test.csv"));
		DataUtils.printLabelHourlyCluster(prTest);
		DataUtils.printLabelHourlyCluster(prTrain);
		String line = br.readLine();
		int count = 0;
		while (line != null) {
			if (line.split(",")[0].equals("age")) {
				line = br.readLine();
				continue;
			} else {
				if (count < 5000) {
					prTest.println(line);
					count++;
				} else {
					prTrain.println(line);
				}
				line = br.readLine();
			}
		}
		prTest.close();
		prTrain.close();
		br.close();
	}

	public static void getTrainData() throws IOException {
		Map<String, List<Double>> mapIdEtc = readMapIdEtc(DataUtils.ID_HOURLY);
		Map<String, Integer> mapIdAge = readMapIdAge(DataUtils.ID_AGE);
		Map<String, String> mapIdAgeRange = rangeMapIdAge(mapIdAge);
		PrintWriter pr = new PrintWriter(new FileWriter(DataUtils.F_AGER_HOURLY));
		DataUtils.printLabelHourlyCluster(pr);
		for (String id : mapIdAgeRange.keySet()) {
			if (mapIdEtc.containsKey(id)) {
				List<Double> hourly = mapIdEtc.get(id);
				if (DataUtils.sum(hourly) > 50) {
					List<Double> hourlyScale = DataUtils.scaleList(hourly);
					pr.print(mapIdAgeRange.get(id));
					DataUtils.printDataHourlyCluster(pr, hourlyScale);
					pr.println();
				}
			}
		}
		pr.close();

	}

	public static void countIdByEtc(String input, String output) throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter(output));
		Map<String, List<Double>> mapIdEtc = readMapIdEtc(input);
		Map<Integer, Integer> result = new HashMap<>();
		for (String id : mapIdEtc.keySet()) {
			Integer numOfArt = (int) DataUtils.round(DataUtils.sum(mapIdEtc.get(id)));
			if (numOfArt > 100000) {
				System.out.println(id + "," + numOfArt);
			}
			if (result.containsKey(numOfArt)) {
				result.put(numOfArt, result.get(numOfArt) + 1);
			} else {
				result.put(numOfArt, 1);
			}
		}
		for (Integer numOfArt : result.keySet()) {
			pr.println(numOfArt + "," + result.get(numOfArt));
		}
		pr.close();
	}

	public static void countIdByAge() throws IOException {
		PrintWriter pr = new PrintWriter(new FileWriter("data/age_count.csv"));
		Map<String, Integer> mapIdAge = readMapIdAge(DataUtils.ID_AGE);
		Map<Integer, Integer> mapAgeCount = new HashMap<>();
		for (String id : mapIdAge.keySet()) {
			Integer age = mapIdAge.get(id);
			if (mapAgeCount.containsKey(age)) {
				mapAgeCount.put(age, mapAgeCount.get(age) + 1);
			} else {
				mapAgeCount.put(age, 1);
			}
		}
		for (Integer age : mapAgeCount.keySet()) {
			pr.println(age + "," + mapAgeCount.get(age));
		}
		pr.close();
	}

	public static Map<String, String> rangeMapIdAge(Map<String, Integer> mapIdAge) {
		Map<String, String> mapIdAgeScale = new HashMap<>();
		for (String key : mapIdAge.keySet()) {
			Integer value = mapIdAge.get(key);
			String valueScale = null;
			if (value <= 18) {
				valueScale = "18-";
			} else if (value >= 19 && value <= 24) {
				valueScale = "19-25";
			} else if (value >= 25 && value <= 34) {
				valueScale = "26-35";
			} else if (value >= 35 && value <= 44) {
				valueScale = "36-45";
			} else if (value >= 45) {
				valueScale = "46+";
			}
			mapIdAgeScale.put(key, valueScale);
		}

		return mapIdAgeScale;
	}

	public static Map<String, List<Double>> scaleMapIdTopicPercent(Map<String, List<Double>> mapIdTopic) {
		Map<String, List<Double>> mapIdTopicScalePercent = new HashMap<>();
		for (String key : mapIdTopic.keySet()) {
			List<Double> listTopic = mapIdTopic.get(key);
			Double sumTopic = DataUtils.sum(listTopic);
			if (sumTopic >= 4 && sumTopic <= 1000) {
				List<Double> listTopicScalePercent = new ArrayList<>();
				for (int i = 0; i < listTopic.size(); i++) {
					Double value = listTopic.get(i);
					Double scaleValuePercent = value / sumTopic;
					listTopicScalePercent.add(scaleValuePercent);
				}
				mapIdTopicScalePercent.put(key, listTopicScalePercent);
			}
		}
		return mapIdTopicScalePercent;
	}

	public static Map<String, Integer> readMapIdAge(String file) throws IOException {
		Map<String, Integer> mapIdAge = new HashMap<>();
		BufferedReader reader = new BufferedReader(new FileReader(file));

		String line = reader.readLine();
		while (line != null) {
			mapIdAge.put(line.split(",")[0], Integer.parseInt(line.split(",")[1]));
			line = reader.readLine();
		}
		reader.close();
		return mapIdAge;
	}

	public static Map<String, List<Double>> readMapIdEtc(String mapType) throws IOException {
		Map<String, List<Double>> result = new HashMap<>();
		int numOfValue = 0;
		switch (mapType) {
		case DataUtils.ID_HOURLY:
			numOfValue = 24;
			break;
		case DataUtils.ID_DAILY:
			numOfValue = 7;
			break;
		case DataUtils.ID_TOPIC:
			numOfValue = 91;
			break;
		}
		BufferedReader br = new BufferedReader(new FileReader(mapType));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr[0].equals("age")) {
				line = br.readLine();
				continue;
			} else if (arr.length == numOfValue + 1) {
				List<Double> listValue = new ArrayList<>();
				for (int i = 1; i <= numOfValue; i++) {
					listValue.add(Double.parseDouble(arr[i]));
				}
				result.put(arr[0], listValue);
				line = br.readLine();
			}
		}
		br.close();
		return result;
	}

}
