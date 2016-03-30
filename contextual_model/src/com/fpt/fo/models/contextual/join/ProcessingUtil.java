package com.fpt.fo.models.contextual.join;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ProcessingUtil {
	public static void main(String[] args) throws IOException, InterruptedException {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Map sortMapByValue(Map unsortMap) {
		List list = new LinkedList(unsortMap.entrySet());

		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Double> normalize(Map<String, Double> map, boolean sorted) {
		Map<String, Double> sortedMap = new HashMap<String, Double>();
		if (sorted) {
			sortedMap = ProcessingUtil.sortMapByValue(map);
		}
		double sum = 0;
		for (String key : sortedMap.keySet()) {
			sum += sortedMap.get(key);
		}
		for (String key : sortedMap.keySet()) {
			sortedMap.put(key, sortedMap.get(key) / sum);
		}
		return sortedMap;
	}

	
}
