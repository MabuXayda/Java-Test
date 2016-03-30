package com.fpt.fo.models.contextual.model.batdongsan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fpt.fo.models.contextual.join.ProcessingUtil;

public class ModelBatDongSan {
	private static final String WORK = System.getProperty("user.dir");
	private static final String FILE_NHA_O = WORK + "/data/keyword_batdongsan/wordNhaO.txt";
	private static final String FILE_KHU_CONG_NGHIEP = WORK + "/data/keyword_batdongsan/wordKhuCongNghiep.txt";
	private static final String FILE_CHUNG_CU = WORK + "/data/keyword_batdongsan/wordChungCu.txt";
	private static final String FILE_MAT_BANG = WORK + "/data/keyword_batdongsan/wordMatBang.txt";
	private static final String SPECIAL_CHARACTER[] = { ",", ".", "?", "-", "&", "!", ";", "(", ")", " " };

	private static ModelBatDongSan instance = null;
	private static Map<String, Set<String>> mapModel = new HashMap<>();

	public static void main(String[] args) throws IOException {
	}

	private ModelBatDongSan() {
		try {
			readMapModel();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static ModelBatDongSan getInstance() {
		if (instance == null) {
			synchronized (ModelBatDongSan.class) {
				if (instance == null) {
					instance = new ModelBatDongSan();
				}
			}
		}
		return instance;
	}

	public Map<String, Double> getMapTopic(List<String> listWord) {
		Map<String, Double> mapTopic = new HashMap<>();
		for (String sentence : listWord) {
			String arr[] = sentence.split(" ");
			for (int i = 0; i < arr.length; i++) {

				String subString;
				StringBuilder tempString = new StringBuilder();
				Boolean checkSpecial = false;
				Boolean checkKeyWord = false;
				for (int j = i; j < i + 4; j++) {
					if (j > arr.length - 1) {
						break;
					}
					for (String specialChar : SPECIAL_CHARACTER) {
						if (arr[j].equals(specialChar)) {
							checkSpecial = true;
							break;
						}
					}
					if (checkSpecial == true) {
						break;
					}
					if (arr[j].length() >= 1) {
						if (j > i) {
							tempString.append("_");
						}
						tempString.append(arr[j].toLowerCase());
					}
					subString = tempString.toString();
					for (String key : mapModel.keySet()) {
						if (subString.equals(key)) {
							// Tiep tuc ghep tu hay bo qua
							// checkKeyWord = true;
							Set<String> setTopic = mapModel.get(key);
							for (String topic : setTopic) {
								if (!mapTopic.containsKey(topic)) {
									mapTopic.put(topic, 1.0);
								} else {
									mapTopic.put(topic, mapTopic.get(topic) + 1.0);
								}
							}
							break;
						}
					}
					if (checkKeyWord == true) {
						break;
					}
				}
			}
		}
		return ProcessingUtil.normalize(mapTopic, true);
	}

	public Map<String, List<String>> getMapTopicKeyWords(List<String> listWord) {
		Map<String, List<String>> mapKeyWord = new HashMap<>();
		for (String sentence : listWord) {
			String arr[] = sentence.split(" ");
			for (int i = 0; i < arr.length; i++) {

				String subString;
				StringBuilder tempString = new StringBuilder();
				Boolean checkSpecial = false;
				Boolean checkKeyWord = false;
				for (int j = i; j < i + 4; j++) {
					if (j > arr.length - 1) {
						break;
					}
					for (String specialChar : SPECIAL_CHARACTER) {
						if (arr[j].equals(specialChar)) {
							checkSpecial = true;
							break;
						}
					}
					if (checkSpecial == true) {
						break;
					}
					if (arr[j].length() >= 1) {
						if (j > i) {
							tempString.append("_");
						}
						tempString.append(arr[j].toLowerCase());
					}
					subString = tempString.toString();
					for (String key : mapModel.keySet()) {
						if (subString.equals(key)) {
							// Tiep tuc ghep tu hay bo qua
							// checkKeyWord = true;
							Set<String> setTopic = mapModel.get(key);
							for (String topic : setTopic) {
								List<String> listKeyWords = new ArrayList<>();
								if (!mapKeyWord.containsKey(topic)) {
									listKeyWords = new ArrayList<>();
									listKeyWords.add(subString);
								} else {
									listKeyWords = mapKeyWord.get(topic);
									listKeyWords.add(subString);
								}
								mapKeyWord.put(topic, listKeyWords);
							}
							break;
						}
					}
					if (checkKeyWord == true) {
						break;
					}
				}
			}
		}
		return mapKeyWord;
	}

	private void readMapModel() throws IOException {
		String files[] = { FILE_NHA_O, FILE_KHU_CONG_NGHIEP, FILE_CHUNG_CU, FILE_MAT_BANG };
		String topic = "";
		for (String file : files) {
			if (file == FILE_NHA_O) {
				topic = "nha_o_nha_tro";
			} else if (file == FILE_KHU_CONG_NGHIEP) {
				topic = "khu_cong_nghiep";
			} else if (file == FILE_CHUNG_CU) {
				topic = "khu_dan_cu_chung_cu";
			} else if (file == FILE_MAT_BANG) {
				topic = "mat_bang_dat_nen";
			}
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			while (line != null) {
				Set<String> setTopic = new HashSet<>();
				if (!mapModel.keySet().contains(line)) {
					setTopic.add(topic);
				} else {
					setTopic = mapModel.get(line);
					setTopic.add(topic);
				}
				mapModel.put(line, setTopic);
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
		}
	}
}
