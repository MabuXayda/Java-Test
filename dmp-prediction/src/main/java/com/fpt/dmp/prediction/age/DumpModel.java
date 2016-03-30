package com.fpt.dmp.prediction.age;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DumpModel {
	public static final String DUMP_MODEL = "data/dumpModel.csv";
	public static final String DUMP_MODEL_FULL = "data/train.csv";
	private Map<String, List<List<Double>>> mapDumpModel = new HashMap<>();
	
	public DumpModel(){
		try {
			loadDumpModel(DUMP_MODEL_FULL);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		// buildDumpModel("data/train.csv");
		buildDumpModelWithSample("data/train.csv", "data/f_hourly_checker.csv");
		// checCosSimilar();
		System.out.println("DONE");
	}

	public static void checCosSimilar() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("data/test_dump.csv"));
		PrintWriter pr = new PrintWriter(new FileWriter("data/test_dump_check.csv"));
		Map<String, Integer> mapCheck = new HashMap<>();
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr[0].equals("age")) {
				line = br.readLine();
				continue;
			} else {
				String valueName = null;
				double value = Double.parseDouble(arr[1]);
				if (value <= 0.1) {
					valueName = "<0.1";
				} else if (value > 0.1 && value <= 0.3) {
					valueName = "<0.3";
				} else if (value > 0.3 && value <= 0.5) {
					valueName = "<0.5";
				} else if (value > 0.5 && value <= 0.6) {
					valueName = "<0.6";
				} else if (value > 0.6 && value <= 0.7) {
					valueName = "<0.7";
				} else if (value > 0.7 && value <= 0.8) {
					valueName = "<0.8";
				} else if (value > 0.8 && value <= 0.9) {
					valueName = "<0.9";
				} else {
					valueName = "0.9-1";
				}
				if (mapCheck.containsKey(valueName)) {
					mapCheck.put(valueName, mapCheck.get(valueName) + 1);
				} else {
					mapCheck.put(valueName, 1);
				}
				line = br.readLine();
			}
		}
		for (String key : mapCheck.keySet()) {
			pr.println(key + "," + mapCheck.get(key));
		}
		br.close();
		pr.close();
	}

	public static void buildDumpModelWithSample(String fileTrain, String fileSample) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileSample));
		Map<String, List<Double>> mapCheck = new HashMap<>();
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr[0].equals("age")) {
				line = br.readLine();
				continue;
			} else {
				List<Double> listCheck = new ArrayList<>();
				for (int i = 1; i < arr.length; i++) {
					listCheck.add(Double.parseDouble(arr[i]));
				}
				mapCheck.put(arr[0], listCheck);
				line = br.readLine();
			}
		}
		PrintWriter pr = new PrintWriter(new FileWriter(DUMP_MODEL));
		BufferedReader brTest = new BufferedReader(new FileReader(fileTrain));
		String lineTest = brTest.readLine();
		while (lineTest != null) {
			String[] arr = lineTest.split(",");
			if (arr[0].equals("age")) {
				pr.println(lineTest);
				lineTest = brTest.readLine();
				continue;
			} else {
				List<Double> listCheck = mapCheck.get(arr[0]);
				List<Double> listCompare = new ArrayList<>();
				for (int i = 1; i < arr.length; i++) {
					listCompare.add(Double.parseDouble(arr[i]));
				}
				double similar = DataUtils.cosineSimilarity(listCheck, listCompare);
				if (similar < 0.8) {
					pr.println(lineTest);
				}
				lineTest = brTest.readLine();
			}
		}
		br.close();
		brTest.close();
		pr.close();
	}

	public static void buildDumpModelNoSample(String fileTrain) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileTrain));
		PrintWriter pr = new PrintWriter(new FileWriter(DUMP_MODEL));
		DataUtils.printFilterLabel(pr);
		Map<String, List<List<Double>>> mapCheck = new HashMap<>();
		String line = br.readLine();
		while (line != null) {
			System.out.println(line);
			List<List<Double>> wholeList = new ArrayList<List<Double>>();
			String[] arr = line.split(",");
			String age = arr[0];
			double similar = 1;
			if (age.equals("age")) {
				line = br.readLine();
				continue;
			} else {
				List<Double> checkList = new ArrayList<Double>();
				for (int i = 1; i < arr.length; i++) {
					checkList.add(Double.parseDouble(arr[i]));
				}
				if (!mapCheck.containsKey(age)) {
					wholeList.add(checkList);
					mapCheck.put(age, wholeList);
					pr.println(line);
				} else {
					wholeList = mapCheck.get(age);
					System.out.println(wholeList.size());
					for (int j = 0; j < wholeList.size(); j++) {
						List<Double> compareList = wholeList.get(j);
						// if (checkList.size() == compareList.size()) {
						similar = DataUtils.cosineSimilarity(checkList, compareList);
						// System.out.println(similar);
						if (similar < 0.3) {
							wholeList.add(checkList);
							pr.println(line);
							mapCheck.put(age, wholeList);
							j = wholeList.size();
						}
						// }
					}
				}
				line = br.readLine();
			}
		}
		br.close();
		pr.close();
	}
	
	public String dumpClassifyByMax(List<Double> input){
		Map<String, Double> mapCheck = new HashMap<>();
		for(String age: mapDumpModel.keySet()){
			mapCheck.put(age, 0.0);
		}
		for(String age: mapDumpModel.keySet()){
			double max = mapCheck.get(age);
//			double sum = mapCheck.get(age);
			List<List<Double>> wholeList = mapDumpModel.get(age);
			for(List<Double> checkList:wholeList){
				double similar = DataUtils.cosineSimilarity(checkList, input);
				if(similar>max){
					max = similar;
				}
//				sum+=similar;
			}
			mapCheck.put(age, max);
//			mapCheck.put(age, sum/wholeList.size());
		}
		return DataUtils.getKeyWithMaxValue(mapCheck);
	}

	private void loadDumpModel(String modelType) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(modelType));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			String age = arr[0];
			List<List<Double>> wholeList = new ArrayList<List<Double>>();
			if (age.equals("age")) {
				line = br.readLine();
				continue;
			} else {
				List<Double> addList = new ArrayList<Double>();
				for (int i = 1; i < arr.length; i++) {
					addList.add(Double.parseDouble(arr[i]));
				}
				if (mapDumpModel.containsKey(age)) {
					wholeList = mapDumpModel.get(age);
					wholeList.add(addList);
					mapDumpModel.put(age, wholeList);
				} else {
					wholeList.add(addList);
					mapDumpModel.put(age, wholeList);
				}
				line = br.readLine();
			}
		}
		br.close();
	}
}
