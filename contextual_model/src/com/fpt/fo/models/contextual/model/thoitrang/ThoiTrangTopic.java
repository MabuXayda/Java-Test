package com.fpt.fo.models.contextual.model.thoitrang;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fpt.fo.models.contextual.join.ProcessingUtil;
import com.fpt.fo.models.contextual.join.UrlProcessing;

public class ThoiTrangTopic {
	UrlProcessing urlProcessing = new UrlProcessing();

	public static void main(String[] args) throws IOException {
		String fileTT = "data/url/urlThoiTrang.txt";
		String fileLD = "data/url/urlLamDep.txt";
		String fileTopic = "data/topic/topicThoiTrangLamDep.txt";
		ThoiTrangTopic ttldTopic = new ThoiTrangTopic();
		ttldTopic.writeSetTopic(fileTT, fileLD, fileTopic);
		System.out.println("DONE");
	}

	public void writeSetTopic(String fileTT, String fileLD, String fileTopic) throws IOException {
		String fileArr[] = { fileTT, fileLD };
		Map<Integer, Integer> mapTopicCount = getMapTopicCount(getUrls(fileArr));
		Set<Integer> setTopic = getSetTopic(mapTopicCount);
		FileWriter fileWriter = new FileWriter(fileTopic);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		for (Integer topic : setTopic) {
			printWriter.print(topic + ",");
		}
		printWriter.close();
	}

	private String[] getUrls(String[] fileArr) throws IOException {
		List<String> listUrl = new ArrayList<>();
		for (String file : fileArr) {
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			while (line != null) {
				listUrl.add(line);
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
		}
		String[] urlsArr = new String[listUrl.size()];
		return listUrl.toArray(urlsArr);
	}

	@SuppressWarnings("unchecked")
	private Map<Integer, Integer> getMapTopicCount(String[] arrUrl) {
		Map<Integer, Integer> synchronizedMapTopicCount = Collections.synchronizedMap(new HashMap<>());
		ExecutorService threadPool = Executors.newFixedThreadPool(5);
		for (String url : arrUrl) {
			System.out.println("PROCESS: " + url);
			Runnable processUrl = new Runnable() {

				@Override
				public void run() {
					String content = urlProcessing.getUrlContent(url);
					Map<Integer, Double> mapVectorTopic = urlProcessing.getVectorTopicSorted(content);
					Integer count = 0;
					for (Integer key : mapVectorTopic.keySet()) {
						if (!synchronizedMapTopicCount.containsKey(key)) {
							synchronizedMapTopicCount.put(key, 1);
						} else {
							synchronizedMapTopicCount.put(key, synchronizedMapTopicCount.get(key) + 1);
						}
						count++;
						if (count == 10) {
							break;
						}
					}
				}
			};
			threadPool.execute(processUrl);
		}
		threadPool.shutdown();
		while (!threadPool.isTerminated()) {
		}
		return ProcessingUtil.sortMapByValue(synchronizedMapTopicCount);
	}

	private Set<Integer> getSetTopic(Map<Integer, Integer> mapTopicCount) {
		Set<Integer> setTopic = new HashSet<>();
		Integer count = 0;
		for (Integer key : mapTopicCount.keySet()) {
			setTopic.add(key);
			count++;
			if (count == 20) {
				break;
			}
		}
		System.out.println(mapTopicCount.toString());
		System.out.println(setTopic.toString());
		return setTopic;
	}
}
