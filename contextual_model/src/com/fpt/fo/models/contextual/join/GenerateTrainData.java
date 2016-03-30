package com.fpt.fo.models.contextual.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GenerateTrainData {
	private static final String URL_SOHOA = "data/classify/URL_SOHOA.txt";
	private static final String URL_DULICH = "data/classify/URL_DULICH.txt";
	private static final String URL_THOITRANG = "data/classify/URL_THOITRANG.txt";
	private static final String URL_BATDONGSAN = "data/classify/URL_BATDONGSAN.txt";

	public static UrlProcessing urlProcessing = new UrlProcessing();

	public static void main(String[] args) throws IOException {
		String[] filesUrl = { URL_SOHOA, URL_DULICH, URL_THOITRANG, URL_BATDONGSAN };
		String fileOutput = "data/trainData.csv";
		writeTrainData(generateTrainData(readListUrl(filesUrl)), fileOutput);
		System.out.println("DONE");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void writeTrainData(List<Entry<String, Map<Integer, Double>>> listUrlVectorTopic, String fileOutput)
			throws IOException {
		FileWriter fileWriter = new FileWriter(fileOutput);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		printWriter.print("Label");
		for (int i = 0; i < 91; i++) {
			printWriter.print("," + i);
		}
		printWriter.println();
		for (Entry entry : listUrlVectorTopic) {
			Map<Integer, Double> mapVectorTopic = (Map<Integer, Double>) entry.getValue();
			Boolean flag = false;
			for (int i = 0; i < 91; i++) {
				if (mapVectorTopic.get(i) == null) {
					flag = true;
					break;
				}
			}
			if (flag) {
				continue;
			}
			String label = (String) entry.getKey();
			printWriter.print(label);
			for (int i = 0; i < 91; i++) {
				printWriter.print("," + mapVectorTopic.get(i));
			}
			printWriter.println();
		}
		printWriter.close();
	}

	public static List<Entry<String, Map<Integer, Double>>> generateTrainData(Map<String, String> mapUrls) {
		List<Entry<String, Map<Integer, Double>>> synchronizedListUrlVectorTopic = Collections
				.synchronizedList(new ArrayList<>());
		final class MyEntry<K, V> implements Map.Entry<K, V> {
			private final K key;
			private V value;

			public MyEntry(K key, V value) {
				this.key = key;
				this.value = value;
			}

			@Override
			public K getKey() {
				return key;
			}

			@Override
			public V getValue() {
				return value;
			}

			@Override
			public V setValue(V value) {
				V old = this.value;
				this.value = value;
				return old;
			}
		}
		ExecutorService threadPool = Executors.newFixedThreadPool(5);
		for (String url : mapUrls.keySet()) {
			Runnable processUrl = new Runnable() {

				@Override
				public void run() {
					Map<Integer, Double> mapVectorTopic = urlProcessing
							.getVectorTopic(urlProcessing.getUrlContent(url));
					@SuppressWarnings({ "unchecked", "rawtypes" })
					Map.Entry<String, Map<Integer, Double>> mapUrlVectorTopic = new MyEntry(mapUrls.get(url),
							mapVectorTopic);
					synchronizedListUrlVectorTopic.add(mapUrlVectorTopic);
				}
			};
			threadPool.execute(processUrl);
		}
		threadPool.shutdown();
		while (!threadPool.isTerminated()) {
		}
		return synchronizedListUrlVectorTopic;
	}

	public static Map<String, String> readListUrl(String[] filesUrl) throws IOException {
		Map<String, String> mapUrls = new HashMap<>();
		for (String fileUrl : filesUrl) {
			String label = fileUrl.split("_")[1].split("\\.")[0];
			FileReader fileReader = new FileReader(fileUrl);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			while (line != null) {
				mapUrls.put(line, label);
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
		}
		return mapUrls;
	}

}
