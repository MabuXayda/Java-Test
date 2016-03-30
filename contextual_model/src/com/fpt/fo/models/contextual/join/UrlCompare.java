package com.fpt.fo.models.contextual.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UrlCompare extends ProcessingUtil {
	private static final String WORK = System.getProperty("user.dir");
	private static final String PATH_SOHOA = WORK + "/data/topic/topicSoHoa.txt";
	private static final String PATH_DULICH = WORK + "/data/topic/topicDuLich.txt";
	private static final String PATH_THOITRANGLAMDEP = WORK + "/data/topic/topicThoiTrangLamDep.txt";

	private static Set<Integer> topicSoHoa = new HashSet<>();
	private static Set<Integer> topicDuLich = new HashSet<>();
	private static Set<Integer> topicThoiTrangLamDep = new HashSet<>();
	private static UrlCompare instance;

	public static void main(String[] args) {
	}

	private UrlCompare() {
		try {
			topicSoHoa = readSetTopic(PATH_SOHOA);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			topicDuLich = readSetTopic(PATH_DULICH);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			topicThoiTrangLamDep = readSetTopic(PATH_THOITRANGLAMDEP);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static UrlCompare getInstance() {
		if (instance == null) {
			synchronized (UrlCompare.class) {
				if (instance == null) {
					instance = new UrlCompare();
				}
			}
		}
		return instance;
	}

	public String classifyUrl(Map<Integer, Double> vectorTopic, Double min) {
		Double sumTopicSohoa = sumMainTopic(vectorTopic, topicSoHoa);
		Double sumTopicDulich = sumMainTopic(vectorTopic, topicDuLich);
		Double sumTopicThoitranglamdep = sumMainTopic(vectorTopic, topicThoiTrangLamDep);

		Map<String, Double> mapCompare = new HashMap<String, Double>();
		mapCompare.put("OTHER", min);

		mapCompare.put("SOHOA", sumTopicSohoa);
		mapCompare.put("DULICH", sumTopicDulich);
		mapCompare.put("THOITRANG", sumTopicThoitranglamdep);
		
		System.out.println(mapCompare.toString());
		return sortMapByValue(mapCompare).keySet().toArray()[0].toString();
	}

	private Double sumMainTopic(Map<Integer, Double> vectorTopic, Set<Integer> topicSet) {
		Double sumMainTopic = 0.0;
		for (Integer key : vectorTopic.keySet()) {
			for (Integer topic : topicSet) {
				if (key == topic) {
					sumMainTopic += vectorTopic.get(topic);
					break;
				}
			}
		}
		return sumMainTopic;
	}

	private Set<Integer> readSetTopic(String topicFile) throws IOException {
		FileReader fileReader = new FileReader(topicFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String[] topicString = bufferedReader.readLine().split(",");
		Set<Integer> setTopic = new HashSet<>();
		for (int i = 0; i < topicString.length; i++) {
			setTopic.add(Integer.parseInt(topicString[i]));
		}
		bufferedReader.close();
		return setTopic;
	}

}
