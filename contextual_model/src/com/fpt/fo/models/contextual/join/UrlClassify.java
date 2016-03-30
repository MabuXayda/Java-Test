package com.fpt.fo.models.contextual.join;

import java.util.Map;

import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class UrlClassify {
	private static final String FILE_MODEL = "data/classify/urlClassifyModel.model";
	private static Instances instances;
	private static Attribute label;

	private static RandomForest randomForest;
	private static UrlClassify instance;

	public static UrlClassify getInstance() {
		if (instance == null) {
			synchronized (UrlClassify.class) {
				if (instance == null) {
					instance = new UrlClassify();
				}
			}
		}
		return instance;
	}

	private UrlClassify() {
		initializeAttribute();
		loadModelRandomForest();
	}

	public String getTopicClassify(Map<Integer, Double> mapVectorTopic) throws Exception {
		if (mapVectorTopic.size() == 91) {
			Instance instance = prepareInstance(mapVectorTopic);
			Double index = randomForest.classifyInstance(instance);
			return label.value(index.intValue());
		} else {
			return "ERROR";
		}

	}

	private Instance prepareInstance(Map<Integer, Double> mapVectorTopic) {
		Instance instance = new Instance(92);
		Integer index = 0;
		instance.setValue(index, Instance.missingValue());
		index++;
		for (int i = 0; i < 91; i++) {
			instance.setValue(index, mapVectorTopic.get(i));
			index++;
		}
		instance.setDataset(instances);
		return instance;
	}

	private void loadModelRandomForest() {
		if (randomForest == null) {
			try {
				System.out.println("Load model: " + FILE_MODEL);
				randomForest = (RandomForest) weka.core.SerializationHelper.read(FILE_MODEL);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}

	private void initializeAttribute() {
		FastVector topicVector = new FastVector();
		topicVector.addElement("SOHOA");
		topicVector.addElement("BATDONGSAN");
		topicVector.addElement("DULICH");
		topicVector.addElement("THOITRANG");
		label = new Attribute("Label", topicVector);
		FastVector attributeVector = new FastVector();
		attributeVector.addElement(label);
		for (int i = 0; i < 91; i++) {
			Attribute topicId = new Attribute(String.valueOf(i));
			attributeVector.addElement(topicId);
		}
		instances = new Instances("Topic_predict", attributeVector, 0);
		instances.setClassIndex(0);
	}

}
