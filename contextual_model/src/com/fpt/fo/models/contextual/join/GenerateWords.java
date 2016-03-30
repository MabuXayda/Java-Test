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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GenerateWords {
	private UrlProcessing urlProcessing = new UrlProcessing();
	private String specialCharacter[] = { ",", ".", "?", "-", "&", "!", ";", "(", ")", " " };

	public static void main(String[] args) throws IOException {
		GenerateWords generateWords = new GenerateWords();
		// String listUrlDL = "data/urlDLKS.txt";
		// String listUrlAT = "data/urlAT.txt";
		// String listUrlTT = "data/urlTTSK.txt";
		// String listUrlThoiTrang = "data/url/urlThoiTrang.txt";
		// String listUrlLamDep = "data/url/urlLamDep.txt";
		String listUrlNhaO = "data/url/BDS_urlNhaO.txt";
		String listUrlChungCu = "data/url/BDS_urlChungCu.txt";
		String listUrlMatBang = "data/url/BDS_urlMatBang.txt";
		String listUrlKhuCongNghiep = "data/url/BDS_urlKhuCongNghiep.txt";

		// String fileOutDL = "data/urlWordAddDL.txt";
		// String fileOutAT = "data/urlWordAddAT.txt";
		// String fileOutTT = "data/urlWordAddTT.txt";
		// String fileOutThoiTrang =
		// "data/keyword_thoitranglamdep/wordThoiTrang.txt";
		// String fileOutLamDep = "data/keyword_thoitranglamdep/wordLamDep.txt";
		String fileOutNhaO = "data/keyword_batdongsan/wordNhaO.txt";
		String fileOutChungCu = "data/keyword_batdongsan/wordChungCu.txt";
		String fileOutMatBang = "data/keyword_batdongsan/wordMatBang.txt";
		String fileOutKhuCongNhiep = "data/keyword_batdongsan/wordKhuCongNghiep.txt";

		// generateWords.generateMapWord(listUrlAT, fileOutAT);
		// generateWords.generateMapWord(listUrlDL, fileOutDL);
		// generateWords.generateMapWord(listUrlTT, fileOutTT);
		// generateWords.generateMapWord(listUrlThoiTrang, fileOutThoiTrang);
		// generateWords.generateMapWord(listUrlLamDep, fileOutLamDep);
		generateWords.generateMapWord(listUrlNhaO, fileOutNhaO);
		generateWords.generateMapWord(listUrlChungCu, fileOutChungCu);
		generateWords.generateMapWord(listUrlMatBang, fileOutMatBang);
		generateWords.generateMapWord(listUrlKhuCongNghiep, fileOutKhuCongNhiep);

		System.out.println("DONE");

	}

	@SuppressWarnings("unchecked")
	public void generateMapWord(String fileIn, String fileOut) throws IOException {
		Map<String, Double> mapWordWeightFinal = ProcessingUtil
				.sortMapByValue(getMapWordWeightFinal(readListUrl(fileIn)));
		FileWriter fileWriter = new FileWriter(fileOut);
		PrintWriter printWriter = new PrintWriter(fileWriter);
		for (String key : mapWordWeightFinal.keySet()) {
			// if (mapWordWeightFinal.get(key) <= 100.0 &&
			// mapWordWeightFinal.get(key) >= 50.0) {
			if (mapWordWeightFinal.get(key) > 1.0) {
				printWriter.println(key + "\t" + mapWordWeightFinal.get(key));
			}
		}
		printWriter.close();
	}

	private List<String> readListUrl(String file) throws IOException {
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();
		List<String> listUrl = new ArrayList<>();
		while (line != null) {
			listUrl.add(line);
			line = bufferedReader.readLine();
		}
		bufferedReader.close();
		return listUrl;
	}

	private Map<String, Double> getMapWordWeightFinal(List<String> listUrl) {
		Map<String, Double> mapWordWeightFinal = Collections.synchronizedMap(new HashMap<>());
		ExecutorService threadPool = Executors.newFixedThreadPool(5);
		for (String url : listUrl) {
			Runnable processurl = new Runnable() {

				@Override
				public void run() {
					System.out.println("PROCESS: " + url);
					String content = urlProcessing.getUrlContent(url);
					List<String> listWord = urlProcessing.getListWord(content);
					Map<String, Double> mapWordWeight = getMapWordWeight(listWord);
					for (String key : mapWordWeight.keySet()) {
						if (!mapWordWeightFinal.keySet().contains(key)) {
							mapWordWeightFinal.put(key, mapWordWeight.get(key));
						} else {
							mapWordWeightFinal.put(key, mapWordWeightFinal.get(key) + mapWordWeight.get(key));
						}
					}
					System.out.println("DONE PROCESS");
				}
			};
			threadPool.execute(processurl);
		}
		threadPool.shutdown();
		while (!threadPool.isTerminated()) {
		}
		return mapWordWeightFinal;
	}
	// private Map<String, Double> getMapWordWeightFinal(List<String> listUrl) {
	// Map<String, Double> mapWordWeightFinal = Collections.synchronizedMap(new
	// HashMap<>());
	//
	// //for (String url : listUrl) {
	// for (int i=16;i<listUrl.size();i++) {
	// String url = listUrl.get(i);
	// System.out.println("PROCESS: " + url + "| " + i);
	// String content = urlProcessing.getUrlContent(url);
	// List<String> listWord = urlProcessing.getListWords(content);
	// Map<String, Double> mapWordWeight = getMapWordWeight(listWord);
	// for (String key : mapWordWeight.keySet()) {
	// if (!mapWordWeightFinal.keySet().contains(key)) {
	// mapWordWeightFinal.put(key, mapWordWeight.get(key));
	// } else {
	// mapWordWeightFinal.put(key, mapWordWeightFinal.get(key) +
	// mapWordWeight.get(key));
	// }
	// }
	// }
	//
	// return mapWordWeightFinal;
	// }

	private Map<String, Double> getMapWordWeight(List<String> listWord) {
		Map<String, Double> mapWord = new HashMap<>();
		for (String sentence : listWord) {
			String arr[] = sentence.split(" ");
			for (int i = 0; i < arr.length; i++) {
				String subString;
				StringBuilder tempString = new StringBuilder();
				Boolean flag = true;
				for (int j = i; j < i + 6; j++) {
					if (j > arr.length - 1) {
						break;
					}
					for (String specialChar : specialCharacter) {
						if (arr[j].equals(specialChar)) {
							flag = false;
							break;
						}
					}
					if (flag == false) {
						break;
					}
					if (arr[j].length() >= 1) {
						if (j > i) {
							tempString.append("_");
						}
						tempString.append(arr[j].toLowerCase());
					}
					subString = tempString.toString();
					if (!mapWord.keySet().contains(subString)) {
						mapWord.put(subString, 1.0);
					} else {
						mapWord.put(subString, mapWord.get(subString) + 1.0);
					}
				}
			}
		}
		return mapWord;
	}
}
