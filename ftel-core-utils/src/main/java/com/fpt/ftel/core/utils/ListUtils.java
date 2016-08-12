package com.fpt.ftel.core.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class ListUtils {
	
	public static void main(String[] args) {
		Set<String> bigSet = new HashSet<>();
		for (int i = 0 ; i < 23; i ++){
			bigSet.add(Integer.toString(i));
		}
		List<Set<String>> listSet = splitSetToSmallerSet(bigSet, 5);
		System.out.println(listSet.size());
		int index = 0;
		for(Set<String> set : listSet){
			System.out.println("index: " + index);
			for(String o : set){
				System.out.println("v: " + o);
			}
			index++;
		}
	}
	
	public static double getMaxListDouble(List<Double> listDouble) {
		double max = listDouble.get(0);
		for (int i = 1; i < listDouble.size(); i++) {
			max = Math.max(max, listDouble.get(i));
		}
		return max;
	}

	public static double getAverageListDouble(List<Double> listDouble) {
		double sum = 0.0;
		for (int i = 0; i < listDouble.size(); i++) {
			sum += listDouble.get(i);
		}
		return sum / listDouble.size();
	}
	
//	public static List<Set<Object>> splitSetToSmallerSet(Set<Object> set, int numPerSet){
//		List<Set<Object>> listSet = new ArrayList<>();
//		int numOfPartition = (int) Math.ceil(set.size() / numPerSet) + 1;
//		for(int i =0; i<numOfPartition; i++){
//			listSet.add(new HashSet<>());
//		}
//		int index = 0;
//		for(Object object : set){
//			listSet.get(index++ % numOfPartition).add(object);
//		}
//		return listSet;
//	}
	
	public static List<Set<String>> splitSetToSmallerSet(Set<String> bigSet, int numPerSet){
		List<Set<String>> listSet = new ArrayList<>();
		Set<String> smallSet = new HashSet<>();
		int count = 0;
		for (String str : bigSet) {
			smallSet.add(str);
			count ++;
			if(count == numPerSet){
				listSet.add(smallSet);
				smallSet = new HashSet<>();
				count = 0;
			}
		}
		listSet.add(smallSet);
		return listSet;
	}
}
