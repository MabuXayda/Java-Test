package com.fpt.fo.models.contextual.model.sohoa;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fpt.fo.models.contextual.join.AccentRemover;
import com.fpt.fo.models.contextual.join.ProcessingUtil;
import com.google.common.base.CharMatcher;

public class ModelSoHoa {
	private static final String WORK = System.getProperty("user.dir");
	private static final String TOPIC_OF_CONCEPT = WORK + "/data/keyword_sohoa/topic_of_keyword.txt";
	private static final String CONCEPT_MODEL = WORK + "/data/keyword_sohoa/model_file.txt";
	private static final String SPECIAL_CHARACTER[] = { ",", ".", "?", "-", "&", "!", ";" };

	private static Map<String, Map<String, String>> conceptCate = new HashMap<>();
	private static Map<String, String> conceptForCompare = new HashMap<>();
	private static Map<String, Map<String, String>> brandCate = new HashMap<>();
	private static Map<String, String> topicInfo = new HashMap<>();
	private static Map<String, String> topicInfoInvert = new HashMap<>();
	private static ModelSoHoa instance = null;

	private Map<String, Double> keywordResult;
	private Map<String, Double> brandResult;
	private Map<String, Double> topicResult;
	private Map<String, Double> keywordFinalResult;

	public static void main(String[] args) {
		ModelSoHoa modelSoHoa = ModelSoHoa.getInstance();
		String text = "Được phát triển từ năm 1991, công nghệ PageWide đã có đóng góp cho thành công của dòng máy in phun HP. Ban đầu, PageWide được dùng cho dòng máy in cỡ nhỏ Officejet nhưng giờ đây dần xuất hiện trên máy in định dạng lớn của HP. Cốt lõi của công nghệ là việc sử dụng đầu in cố định giúp máy in nhanh hơn.";
		Map<String, Double> result = modelSoHoa.getMapTopic(text);
		System.out.println(result);
	}

	private ModelSoHoa() {
		try {
			loadConceptModel(CONCEPT_MODEL);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			loadTopicOfConcept(TOPIC_OF_CONCEPT);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static ModelSoHoa getInstance() {
		if (instance == null) {
			synchronized (ModelSoHoa.class) {
				if (instance == null) {
					instance = new ModelSoHoa();
				}
			}
		}
		return instance;
	}

	private void loadConceptModel(String modelFile) throws IOException {
		FileReader fileReader = new FileReader(modelFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();

		while (line != null) {
			String arr[] = line.toLowerCase().split("\t");
			String keyword = arr[2];
			String keywordForCompare = arr[2].replace(" ", "").replace("_", "");

			// load original keyword map
			if (!conceptCate.containsKey(keyword)) {
				conceptCate.put(keyword, new HashMap<String, String>());
			}
			conceptCate.get(keyword).put(arr[1], arr[0]);

			// load keyword for comparing
			if (!conceptForCompare.containsKey(keywordForCompare)) {
				conceptForCompare.put(keywordForCompare, keyword);
			}

			if (!brandCate.containsKey(arr[1])) {
				brandCate.put(arr[1], new HashMap<String, String>());
			}
			brandCate.get(arr[1]).put(arr[0], "1.0");

			line = bufferedReader.readLine();
		}
		bufferedReader.close();
	}

	private void loadTopicOfConcept(String topicFile) throws IOException {
		FileReader fileReader = new FileReader(topicFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();
		while (line != null) {
			String arr[] = line.split("\t");
			topicInfo.put(arr[0], arr[1].toLowerCase());

			String name[] = arr[1].split(",");
			for (String s : name) {
				topicInfoInvert.put(s.toLowerCase(), arr[0]);
			}

			line = bufferedReader.readLine();
		}
		bufferedReader.close();
	}

	private void updateBasicKeywordBrandTopic(String content) {
		keywordResult = new HashMap<>();
		brandResult = new HashMap<>();
		topicResult = new HashMap<>();
		String sentences[] = content.split("\n");
		for (String sentence : sentences) {
			String atts[] = sentence.split(" ");
			int iTemp = 0;

			for (int i = 0; i < atts.length; i++) {
				String subString = "";
				String selectedString = "";
				String selectedBrandString = "";
				String selectedTopicString = "";

				for (int j = i; j < i + 6; j++) {
					if (j > atts.length - 1) {
						break;
					}
					// atts[j] = CharMatcher.inRange('0',
					// '9').or(CharMatcher.inRange('a', 'z'))
					// .or(CharMatcher.inRange('A', 'Z')).retainFrom(atts[j]);
					for (String s : SPECIAL_CHARACTER) {
						if (atts[j].contains(s)) {
							atts[j] = atts[j].replace(s, "");
						}
					}

					if (atts[j].length() >= 1) {
						subString += AccentRemover.removeAccent(atts[j]).toLowerCase();
					}
					if (subString != null && !subString.isEmpty() && subString.length() >= 1) {
						if (brandCate.containsKey(subString)) {
							selectedBrandString = subString;
							if (j > iTemp) {
								iTemp = j;
							}
						}
						if (conceptForCompare.containsKey(subString)) {
							selectedString = subString;
							if (j > iTemp) {
								iTemp = j;
							}
						}
						if (topicInfoInvert.containsKey(subString)) {
							selectedTopicString = subString;
							if (j > iTemp) {
								iTemp = j;
							}
						}
					} else {
						subString = "";
						break;
					}

				}

				if (conceptForCompare.containsKey(selectedString)) {
					String conceptKey = conceptForCompare.get(selectedString);
					if (!keywordResult.containsKey(conceptKey)) {
						keywordResult.put(conceptKey, 1.0);
					} else {
						keywordResult.put(conceptKey, keywordResult.get(conceptKey) + 1.0);
					}
				}
				if (brandCate.containsKey(selectedBrandString)) {
					if (!brandResult.containsKey(selectedBrandString)) {
						brandResult.put(selectedBrandString, 1.0);
					} else {
						brandResult.put(selectedBrandString, brandResult.get(selectedBrandString) + 1.0);
					}
				}
				if (topicInfoInvert.containsKey(selectedTopicString)) {
					String keyTopicOut = topicInfo.get(topicInfoInvert.get(selectedTopicString)).split(",")[0];
					if (!topicResult.containsKey(keyTopicOut)) {
						topicResult.put(keyTopicOut, 1.0);
					} else {
						topicResult.put(keyTopicOut, topicResult.get(keyTopicOut) + 1.0);
					}
				}

				if (iTemp > i) {
					i = iTemp;
				}
			}
		}
	}

	private void updateFinalKeywordBrandTopic() {
		keywordFinalResult = new HashMap<>();
		for (String k : keywordResult.keySet()) {
			String kResult = checkSubKey(k, keywordResult);
			if (kResult != "") {
				if (!keywordFinalResult.containsKey(kResult)) {
					keywordFinalResult.put(kResult, keywordResult.get(k));
				} else {
					keywordFinalResult.put(kResult, keywordFinalResult.get(kResult) + keywordResult.get(k));
				}
			}
		}

		for (String k : keywordFinalResult.keySet()) {
			Map<String, String> info = conceptCate.get(k);
			for (String b : info.keySet()) {
				if (!brandResult.containsKey(b)) {
					brandResult.put(b, keywordFinalResult.get(k) + 1.0);
				} else {
					brandResult.put(b, brandResult.get(b) + keywordFinalResult.get(k));
				}

				String t = info.get(b);
				if (!topicResult.containsKey(topicInfo.get(t).split(",")[0])) {
					topicResult.put(topicInfo.get(t).split(",")[0], keywordFinalResult.get(k) + 1.0);
				} else {
					topicResult.put(topicInfo.get(t).split(",")[0],
							topicResult.get(topicInfo.get(t).split(",")[0]) + keywordFinalResult.get(k));
				}
			}
		}
	}

	public Map<String, Map<String, Double>> getKeywordOfArticleByName(String content) {
		updateBasicKeywordBrandTopic(content);
		updateFinalKeywordBrandTopic();
		Map<String, Map<String, Double>> result = new HashMap<>();
		result.put("brand", ProcessingUtil.normalize(brandResult, true));
		result.put("keyword", ProcessingUtil.normalize(keywordFinalResult, true));
		result.put("topic", ProcessingUtil.normalize(topicResult, true));
		return result;
	}

	public Map<String, Double> getMapTopic(String content) {
		updateBasicKeywordBrandTopic(content);
		updateFinalKeywordBrandTopic();
		Map<String, Double> mapTopic = new HashMap<>();
		mapTopic.putAll(ProcessingUtil.normalize(brandResult, true));
		mapTopic.putAll(ProcessingUtil.normalize(keywordFinalResult, true));
		mapTopic.putAll(ProcessingUtil.normalize(topicResult, true));
		return mapTopic;
	}

	private String checkSubKey(String k, Map<String, Double> map) {
		String result = "";
		for (String kMap : map.keySet()) {
			int lengKMap = kMap.length();
			int lengK = k.length();
			boolean flag = true;
			String kIterms[] = k.split("_");
			String kMapIterms[] = kMap.split("_");
			if (lengK > lengKMap) {
				if (k.contains(kMap)) {
					// find the first index of first element in kMap
					int firstIndex = Arrays.asList(kIterms).indexOf(kMapIterms[0]);
					if (firstIndex >= 0) {
						for (int i = 1; i < kMapIterms.length; i++) {
							if (!kIterms[firstIndex + i].equalsIgnoreCase(kMapIterms[i])) {
								flag = false;
								break;
							}
						}
					}
				} else {
					flag = false;
				}
				if (flag) {
					if (result.length() < k.length()) {
						result = k;
					}
				}
			} else {
				if (kMap.contains(k)) {
					// find the first index of first element in kMap
					int firstIndex = Arrays.asList(kMapIterms).indexOf(kIterms[0]);
					if (firstIndex >= 0) {
						for (int i = 1; i < kIterms.length; i++) {
							if (!kMapIterms[firstIndex + i].equalsIgnoreCase(kIterms[i])) {
								flag = false;
								break;
							}
						}
					}
				} else {
					flag = false;
				}
				if (flag) {
					if (result.length() < kMap.length()) {
						result = kMap;
					}
				}
			}
		}
		return result;
	}

}
