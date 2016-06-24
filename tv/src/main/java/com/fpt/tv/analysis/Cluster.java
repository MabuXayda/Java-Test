package com.fpt.tv.analysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.fpt.tv.utils.AnalysisUtils;
import com.fpt.tv.utils.CommonConfig;
import com.fpt.tv.utils.Utils;

public class Cluster {
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
				Map<String, Double> mapInfoScale = Utils.scaleMap(mapInfo);
				if (setUserActive.contains(customerId)) {
					mapUserActiveInfo.put(customerId, mapInfoScale);
				} else if (setUserChurn.contains(customerId)) {
					mapUserChurnInfo.put(customerId, mapInfoScale);
				}
			}
			line = br.readLine();
		}
		br.close();
		calculateKLCluster(mapUserActiveInfo, mapUserChurnInfo);
	}

	public void calculateKLCluster(Map<String, Map<String, Double>> mapUserActiveInfo,
			Map<String, Map<String, Double>> mapUserChurnInfo) throws IOException {
		Map<String, Double> mapSimilarAvg = Collections.synchronizedMap(new HashMap<>());
		Map<String, Double> mapSimilarMax = Collections.synchronizedMap(new HashMap<>());

		AtomicInteger count = new AtomicInteger(0);
		ExecutorService executorService = Executors.newFixedThreadPool(4);

		for (String idActive : mapUserActiveInfo.keySet()) {
			executorService.execute(new Runnable() {

				@Override
				public void run() {
					List<Double> listSimilar = new ArrayList<>();
					Map<String, Double> mapSimilarMain = mapUserActiveInfo.get(idActive);
					for (String idChurn : mapUserChurnInfo.keySet()) {
						Map<String, Double> mapSimilarCompare = mapUserChurnInfo.get(idChurn);
						double klDistance = AnalysisUtils.getKLDistance(mapSimilarMain, mapSimilarCompare);
						listSimilar.add(klDistance);
					}
					double avg = Utils.getAverageListDouble(listSimilar);
					double max = Utils.getMaxListDouble(listSimilar);
					mapSimilarAvg.put(idActive, avg);
					mapSimilarMax.put(idActive, max);
					count.incrementAndGet();
//					System.out.println(count + " | " + idActive + " | Avg: " + avg + " | Max: " + max);
					if(count.get() % 5000 == 0){
						System.out.println(count);
					}
				}
			});
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
		}
		PrintWriter prAvg = Utils
				.getPrintWriter(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/similarAvg.csv");
		PrintWriter prMax = Utils
				.getPrintWriter(CommonConfig.getInstance().get(CommonConfig.MAIN_DIR) + "/similarMax.csv");
		prAvg.println("CustomerId,KLDistanceAvg");
		prMax.println("CustomerId,KLDistanceMax");
		for (String customerId : mapUserActiveInfo.keySet()) {
			prAvg.println(customerId + "," + mapSimilarAvg.get(customerId));
			prMax.println(customerId + "," + mapSimilarMax.get(customerId));
		}
		prAvg.close();
		prMax.close();

	}

}
