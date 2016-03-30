package com.fpt.fo.models.contextual.join;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.fpt.dm.contextual.IntegratedContextProcess;
import com.fpt.dm.crawler.CrawlerServiceImpl;
import com.fpt.fo.models.contextual.model.batdongsan.ModelBatDongSan;
import com.fpt.fo.models.contextual.model.dulich.ModelDuLich;
import com.fpt.fo.models.contextual.model.sohoa.ModelSoHoa;
import com.fpt.fo.models.contextual.model.thoitrang.ModelThoiTrang;

public class UrlProcessing {
	final static Logger LOGGER = Logger.getLogger(UrlProcessing.class);
	private static final String WORK = System.getProperty("user.dir");
	private static final String PATH_NLP = WORK + "/resources/nlp_model";
	private static final String PATH_LDA = WORK + "/resources/lda_model";
	private static final String PATH_POS = WORK + "/resources/pos_tagger_model/model/crfs";

	private static CrawlerServiceImpl crawlerServiceImpl;
	private static IntegratedContextProcess integratedContextProcess;
	private static UrlCompare urlCompare;
	private static UrlClassify urlClassify;

	private static ModelSoHoa modelSoHoa;
	private static ModelDuLich modelDuLich;
	private static ModelThoiTrang modelThoiTrangLamDep;
	private static ModelBatDongSan modelBatDongSan;

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		UrlProcessing urlProcessing = new UrlProcessing();
		LOGGER.error("this is ANOTHER error");

		urlProcessing.init();
		String url = "http://vitalk.vn/";

		System.out.println("---URL CONTENT---");
		String urlContent = urlProcessing.getUrlContent(url);
		System.out.println(urlContent);
		
		System.out.println("---VECTOR KEYWORD---");
		Map vectorKeyword = urlProcessing.getPOS(urlContent);
		System.out.println(vectorKeyword);
		
//		System.out.println("---VECTOR TOPIC---");
//		Map<Integer, Double> mapVectorTopic = ProcessingUtil.sortMapByValue(urlProcessing.getVectorTopic(urlContent));
//		System.out.println(mapVectorTopic);
//
//		System.out.println("---CLASSIFY---");
//		String urlClassify = urlProcessing.getUrlClassify(mapVectorTopic);
//		System.out.println(urlClassify);
//
//		System.out.println("---TOPIC---");
//		Map<String, Double> urlTopic = urlProcessing.getUrlContext(urlContent, urlClassify);
//		System.out.println(urlTopic.toString());
//
//		System.out.println();
//		System.out.println("DONE");
	}
	
	public Map getPOS(String text){
		return integratedContextProcess.getPOSNewForTest(text);
	}

	public UrlProcessing() {
		crawlerServiceImpl = new CrawlerServiceImpl();
		PropertyConfigurator.configure("./configs/log4j.properties");
		integratedContextProcess = IntegratedContextProcess.getInstance(PATH_NLP, PATH_LDA, PATH_POS);
	}

	public void init() {
		urlCompare = UrlCompare.getInstance();
		urlClassify = UrlClassify.getInstance();
		modelSoHoa = ModelSoHoa.getInstance();
		modelDuLich = ModelDuLich.getInstance();
		modelThoiTrangLamDep = ModelThoiTrang.getInstance();
		modelBatDongSan = ModelBatDongSan.getInstance();
	}

	public String getUrlContent(String url) {
		return crawlerServiceImpl.crawlerUrl(url, true, false);
	}

	public List<String> getListSentence(String urlContent) {
		return integratedContextProcess.getSentences(urlContent);
	}

	public List<String> getListWord(String urlContent) {
		return integratedContextProcess.getVnWords(urlContent);
	}

	public List<String> getListWordParsing(String urlContent) {
		return integratedContextProcess.getPOS(urlContent);
	}

	@SuppressWarnings("unchecked")
	public Map<Integer, Double> getVectorTopicSorted(String urlContent) {
		List<Entry<Integer, Double>> listVectorTopic = integratedContextProcess.getTopics(urlContent);
		Map<Integer, Double> mapVectorTopic = new HashMap<>();
		for (Entry<Integer, Double> entry : listVectorTopic) {
			mapVectorTopic.put(entry.getKey(), entry.getValue());
		}

		return ProcessingUtil.sortMapByValue(mapVectorTopic); // map sorted
	}

	public Map<Integer, Double> getVectorTopic(String urlContent) {
		List<Entry<Integer, Double>> listVectorTopic = integratedContextProcess.getTopics(urlContent);
		Map<Integer, Double> mapVectorTopic = new HashMap<>();
		for (Entry<Integer, Double> entry : listVectorTopic) {
			mapVectorTopic.put(entry.getKey(), entry.getValue());
		}

		return mapVectorTopic; // map unsorted
	}

	public String getUrlCompare(Map<Integer, Double> mapVectorTopic, Double minCompare) {
		return urlCompare.classifyUrl(mapVectorTopic, minCompare);
	}

	public String getUrlCompare(Map<Integer, Double> mapVectorTopic) {
		return urlCompare.classifyUrl(mapVectorTopic, 0.5);
	}

	public String getUrlClassify(Map<Integer, Double> mapVectorTopic) throws Exception {
		return urlClassify.getTopicClassify(mapVectorTopic);
	}

	public Map<String, Double> getUrlContext(String urlContent, String urlClassify) {
		Map<String, Double> mapTopic = new HashMap<>();
		if (urlClassify == "SOHOA") {
			mapTopic = modelSoHoa.getMapTopic(urlContent);
		} else if (urlClassify == "DULICH") {
			mapTopic = modelDuLich.getMapTopic(getListWord(urlContent));
		} else if (urlClassify == "THOITRANG") {
			mapTopic = modelThoiTrangLamDep.getMapTopic(getListWord(urlContent));
		} else if (urlClassify == "BATDONGSAN") {
			mapTopic = modelBatDongSan.getMapTopic(getListWord(urlContent));
		} else if (urlClassify == "ERROR") {
			System.out.println("ERROR: Empty Vector Topic");
			mapTopic = Collections.emptyMap();
		}
		return mapTopic;
	}

}
