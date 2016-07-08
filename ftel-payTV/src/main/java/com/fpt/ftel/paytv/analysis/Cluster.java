package com.fpt.ftel.paytv.analysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.fpt.ftel.core.config.CommonConfig;
import com.fpt.ftel.core.utils.ListUtils;
import com.fpt.ftel.core.utils.MapUtils;
import com.fpt.ftel.core.utils.StringUtils;
import com.fpt.ftel.paytv.utils.AnalysisUtils;
import com.fpt.ftel.paytv.utils.PayTVUtils;

public class Cluster {
	public static List<String> LIST_FEATURES = Arrays.asList("Time1", "Time2", "Time3", "IPTV", "VOD_TOTAL", "SERVICE",
			"LOGID_TIMESHIFT", "LOGID_SERVICE", "ReuseCount", "ReuseAvg", "ReuseMax", "DayActive");

	public static void main(String[] args) throws IOException {
		Cluster cluster = new Cluster();
		cluster.process();
		System.out.println("DONE");

	}

	public void test() {
		Map<String, Double> p1 = new HashMap<>();
		Map<String, Double> p2 = new HashMap<>();
		double[] test1 = new double[] { 8222, 14775, 136925, 117316, 42606, 0, 0, 0, 24, 0, 0, 1, 0, 0, 310 };
		double[] test2 = new double[] { 1127, 62784, 49740, 91116, 6862, 13, 0, 15660, 0, 0, 31, 0, 0, 0, 72 };
		for (int i = 0; i < 15; i++) {
			p1.put(Integer.toBinaryString(i + 1), test1[i]);
			p2.put(Integer.toBinaryString(i + 1), test2[i]);
		}

		System.out.println(AnalysisUtils.getCosinSimilarity(p1, p2));
	}

	public void process() throws IOException {
		Map<String, Map<String, Double>> mapUserActiveInfo = new HashMap<>();
		Map<String, Map<String, Double>> mapUserChurnInfo = new HashMap<>();
		Map<String, Map<String, Double>> mapUserTestInfo = new HashMap<>();

		loadMapInfoFromTrain(mapUserActiveInfo, mapUserChurnInfo,
				CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/train_test/7_train_scale.csv");
		loadMapInfoFromTest(mapUserTestInfo,
				CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/train_test/7_test_scale.csv");
		
//		System.out.println("Process similar user Active");
//		calculateSimilarity(mapUserActiveInfo, mapUserChurnInfo,
//				CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/train_similarAvg.csv",
//				CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/train_similarMax.csv");
		System.out.println("Process similar user Test");
		calculateSimilarity(mapUserTestInfo, mapUserChurnInfo,
				CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/test_similarAvg.csv",
				CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/test_similarMax.csv");
	}

	public void loadMapActiveChurnFromVectorHourly(Map<String, Map<String, Double>> mapUserActiveInfo,
			Map<String, Map<String, Double>> mapUserChurnInfo, String vectorHourlyPath) throws IOException {
		Set<String> setUserActive = new HashSet<>();
		Set<String> setUserChurn = new HashSet<>();

		BufferedReader br = new BufferedReader(
				new FileReader(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/userActive.csv"));
		String line = br.readLine();
		while (line != null) {
			setUserActive.add(line);
			line = br.readLine();
		}
		br.close();
		br = new BufferedReader(
				new FileReader(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/userChurn.csv"));
		line = br.readLine();
		while (line != null) {
			setUserChurn.add(line);
			line = br.readLine();
		}
		br.close();

		br = new BufferedReader(
				new FileReader(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/z_train/vectorHourly.csv"));
		line = br.readLine();
		line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			if (arr.length == 25) {
				String customerId = arr[0];
				Map<String, Double> mapInfo = new HashMap<>();
				for (int i = 1; i < arr.length; i++) {
					double value = 0;
					if (!arr[i].equals("null")) {
						value = Double.parseDouble(arr[i]);
					}
					mapInfo.put(Integer.toString(i), value);
				}
				Map<String, Double> mapInfoScale = MapUtils.scaleMap(mapInfo, PayTVUtils.TIME_USE_TOP);
				if (setUserActive.contains(customerId)) {
					mapUserActiveInfo.put(customerId, mapInfoScale);
				} else if (setUserChurn.contains(customerId)) {
					mapUserChurnInfo.put(customerId, mapInfoScale);
				}
			}
			line = br.readLine();
		}
		br.close();

	}

	public void loadMapInfoFromTest(Map<String, Map<String, Double>> mapUserTestInfo, String fileTestPath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileTestPath));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			int size = arr.length;
			String customerId = arr[0];
			if (StringUtils.isNumeric(customerId)) {
				Map<String, Double> mapInfo = new HashMap<>();
				for (int i = 1; i < size - 1; i++) {
					mapInfo.put(LIST_FEATURES.get(i - 1), Double.parseDouble(arr[i]));
				}
				mapUserTestInfo.put(customerId, mapInfo);
			}
			line = br.readLine();
		}
		br.close(); 
	}

	public void loadMapInfoFromTrain(Map<String, Map<String, Double>> mapUserActiveInfo,
			Map<String, Map<String, Double>> mapUserChurnInfo, String fileTrainPath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileTrainPath));
		String line = br.readLine();
		while (line != null) {
			String[] arr = line.split(",");
			int size = arr.length;
			String customerId = arr[0];
			String churn = arr[size - 1];
			if (StringUtils.isNumeric(customerId)) {
				Map<String, Double> mapInfo = new HashMap<>();
				for (int i = 1; i < size - 1; i++) {
					mapInfo.put(LIST_FEATURES.get(i - 1), Double.parseDouble(arr[i]));
				}
				if (churn.equals("True")) {
					mapUserChurnInfo.put(customerId, mapInfo);
				} else if (churn.equals("False")) {
					mapUserActiveInfo.put(customerId, mapInfo);
				}
			}
			line = br.readLine();
		}
		br.close();
	}

	public void calculateSimilarity(Map<String, Map<String, Double>> mapUserCheckInfo,
			Map<String, Map<String, Double>> mapUserCompareInfo, String outputAvg, String outputMax) throws IOException {
		Map<String, Double> mapSimilarAvg = Collections.synchronizedMap(new HashMap<>());
		Map<String, Double> mapSimilarMax = Collections.synchronizedMap(new HashMap<>());

		AtomicInteger count = new AtomicInteger(0);
		ExecutorService executorService = Executors.newFixedThreadPool(4);

		for (String idActive : mapUserCheckInfo.keySet()) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					List<Double> listSimilar = new ArrayList<>();
					Map<String, Double> mapSimilarMain = mapUserCheckInfo.get(idActive);
					for (String idChurn : mapUserCompareInfo.keySet()) {
						Map<String, Double> mapSimilarCompare = mapUserCompareInfo.get(idChurn);
						double klDistance = AnalysisUtils.getKLDistance(mapSimilarMain, mapSimilarCompare);
						listSimilar.add(klDistance);
					}
					double avg = ListUtils.getAverageListDouble(listSimilar);
					double max = ListUtils.getMaxListDouble(listSimilar);
					mapSimilarAvg.put(idActive, avg);
					mapSimilarMax.put(idActive, max);
					count.incrementAndGet();
					// System.out.println(count + " | " + idActive + " | Avg: "
					// + avg + " | Max: " + max);
					if (count.get() % 5000 == 0) {
						System.out.println(count);
					}
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}

		PrintWriter prAvg = new PrintWriter(new FileWriter(outputAvg));
		PrintWriter prMax = new PrintWriter(new FileWriter(outputMax));
		prAvg.println("CustomerId,KLDistanceAvg");
		prMax.println("CustomerId,KLDistanceMax");
		for (String customerId : mapUserCheckInfo.keySet()) {
			prAvg.println(customerId + "," + mapSimilarAvg.get(customerId));
			prMax.println(customerId + "," + mapSimilarMax.get(customerId));
		}
		prAvg.close();
		prMax.close();

	}

}
