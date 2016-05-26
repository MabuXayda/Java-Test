package com.fpt.tv.utils;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ActionUtils {
	
	public static void printCountItem(PrintWriter pr, Set<String> setItem,
			Map<String, Map<String, Integer>> totalMapCountItem) {
		List<String> listItem = new ArrayList<>(setItem);
		pr.print("CustomerId");
		for (String item : listItem) {
			pr.print("," + item);
		}
		pr.println();
		for (String customerId : totalMapCountItem.keySet()) {
			pr.print(customerId);
			for (String item : listItem) {
				pr.print(" , " + totalMapCountItem.get(customerId).get(item));
			}
			pr.println();
		}
		pr.close();
	}
	
	public static void updateCountItem(Map<String, Integer> mapCountItem, String item) {
		if (mapCountItem.containsKey(item)) {
			mapCountItem.put(item, mapCountItem.get(item) + 1);
		} else {
			mapCountItem.put(item, 1);
		}
	}
}
