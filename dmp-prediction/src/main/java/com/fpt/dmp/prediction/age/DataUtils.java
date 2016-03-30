package com.fpt.dmp.prediction.age;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DataUtils {

	public static final int HOUR = 24;
	public static final int DAY = 7;
	public static final int TOPIC = 91;
	public static final List<Integer> HOUR_FILTER = Arrays.asList(0, 5, 8, 12, 16, 17, 20, 21, 22, 23);

	public static final String ID_BIRTH = "data/birth.csv";
	public static final String ID_AGE = "data/id_age.csv";
	public static final String ID_HOURLY = "data/id_hourly.csv";
	public static final String ID_DAILY = "data/id_daily.csv";
	public static final String ID_TOPIC = "data/id_topic.csv";

	public static final String AGER_HOURLY = "data/ager_hourly.csv";
	public static final String F_AGER_HOURLY = "data/f_ager_hourly.csv";
	public static final String J_AGER_HOURLY = "data/j_ager_hourly.csv";
	public static final String AGER_DAILY = "data/ager_daily.csv";
	public static final String AGER_TOPIC = "data/ager_topic.csv";

	public static void main(String[] args) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
		System.out.println(dateFormat.format(calendar.getTime()));
		List<Double> a = Arrays.asList(4.54, 2.83, 13.7, 13.54, 10.9, 9.11, 11.71, 13.32, 12.25, 8.1);
		List<Double> b = Arrays.asList(3.4, 1.7, 5.35, 6.11, 2.12, 0.93, 8.66, 4.5, 3.9, 3.99);
		System.out.println(cosineSimilarity(a, b));
	}

	public static String getKeyWithMaxValue(Map<String, Double> mapInput) {
		Map.Entry<String, Double> maxEntry = null;
		for (Map.Entry<String, Double> entry : mapInput.entrySet()) {
			if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
				maxEntry = entry;
			}
		}
		return maxEntry.getKey();
	}

	public static double cosineSimilarity(List<Double> vectorA, List<Double> vectorB) {
		double dotProduct = 0.0;
		double normA = 0.0;
		double normB = 0.0;
		for (int i = 0; i < vectorA.size(); i++) {
			dotProduct += vectorA.get(i) * vectorB.get(i);
			normA += Math.pow(vectorA.get(i), 2);
			normB += Math.pow(vectorB.get(i), 2);
		}
		double check = (Math.sqrt(normA) * Math.sqrt(normB));
		if (check == 0) {
			return 0.0;
		} else {
			return dotProduct / check;
		}
	}

	public static void printLabelHourlyCluster(PrintWriter pr) {
		List<String> list = Arrays.asList("23-1", "2-4", "5-7", "8-10", "11-13", "14-16", "17-19", "20-22");
		pr.print("age");
		for (int i = 0; i < list.size(); i++) {
			pr.print("," + list.get(i));
		}
		pr.println();
	}

	public static void printDataHourlyCluster(PrintWriter pr, List<Double> list) {
		if (list.size() == 24) {
			double v1 = list.get(23) + list.get(0) + list.get(1);
			double v2 = list.get(2) + list.get(3) + list.get(4);
			double v3 = list.get(5) + list.get(6) + list.get(7);
			double v4 = list.get(8) + list.get(9) + list.get(10);
			double v5 = list.get(11) + list.get(12) + list.get(13);
			double v6 = list.get(14) + list.get(15) + list.get(16);
			double v7 = list.get(17) + list.get(18) + list.get(19);
			double v8 = list.get(20) + list.get(21) + list.get(22);
			List<Double> listHourlyCluster = scaleList(Arrays.asList(v1, v2, v3, v4, v5, v6, v7, v8));
			for (int i = 0; i < 8; i++) {
				pr.print("," + round(listHourlyCluster.get(i)));
			}
		}
	}

	public static void printLabel(PrintWriter pr, int n) {
		pr.print("age");
		for (int i = 0; i < n; i++) {
			pr.print("," + i);
		}
		pr.println();
	}

	public static void printFilterLabel(PrintWriter pr) {
		pr.print("age");
		for (int i = 0; i < HOUR_FILTER.size(); i++) {
			pr.print("," + HOUR_FILTER.get(i));
		}
		pr.println();
	}

	public static Integer getYearFromLongnumber(String stringNumber) {
		Long longNumber = Long.parseLong(stringNumber, 10);
		Date date = new Date(longNumber * 1000);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.YEAR);
	}

	public static Integer getYearFromString(String date, String splitChar) {
		String[] arr = date.split(splitChar);
		return Integer.parseInt(arr[2]);
	}

	public static Integer getAgeFromYear(Integer year) {
		Integer currentYear = Calendar.getInstance().get(Calendar.YEAR);
		return currentYear - year;
	}

	public static double round(double d) {
		return Math.round(d * 100.0) / 100.0;
	}

	public static List<Double> scaleList(List<Double> list1) {
		List<Double> result = new ArrayList<>();
		double sum = sum(list1);
		for (int i = 0; i < list1.size(); i++) {
			double scaleValue = (list1.get(i) / sum) * 100.0;
			result.add(scaleValue);
		}
		return result;
	}

	public static List<Double> plus2List(List<Double> list1, List<Double> list2) {
		List<Double> result = new ArrayList<>();
		if (list1.size() == list2.size()) {
			for (int i = 0; i < list1.size(); i++) {
				result.add(list1.get(i) + list2.get(i));
			}
			return result;
		} else {
			return null;
		}
	}

	public static double sum(List<Double> list) {
		if (list == null || list.size() < 1) {
			return 0.0;
		}
		Double sum = 0.0;
		for (Double d : list) {
			sum = sum + d;
		}
		return sum;
	}
}
