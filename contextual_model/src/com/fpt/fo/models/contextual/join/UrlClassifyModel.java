package com.fpt.fo.models.contextual.join;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;

public class UrlClassifyModel {
	private static final String FILE_CSV = "data/classify/urlClassifyTrainData.csv";
	private static final String FILE_ARFF = "data/classify/urlClassifyTrainData.arff";
	private static final String FILE_MODEL = "data/classify/urlClassifyModel.model";

	public static void main(String[] args) throws Exception {
		convertCsvToArff(FILE_CSV, FILE_ARFF);
		trainRandomForest(FILE_ARFF, FILE_MODEL);
		System.out.println("DONE");
	}

	public static void trainRandomForest(String fileArff, String outputModel) throws Exception {
		FileReader fileReader = new FileReader(fileArff);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		Instances trainData = new Instances(bufferedReader);
		trainData.setClassIndex(0);

		Integer numTrees = 100;

		System.out.println("----- RANDOM FOREST");
		RandomForest randomForest = new RandomForest();
		randomForest.setNumTrees(numTrees);

		randomForest.buildClassifier(trainData);
		SerializationHelper.write(outputModel, randomForest);
		System.out.println("Save trained model to: " + outputModel);
		bufferedReader.close();
	}

	public static void convertCsvToArff(String inputCsv, String outputArff) throws IOException {
		CSVLoader csvLoader = new CSVLoader();
		csvLoader.setSource(new File(inputCsv));
		Instances data = csvLoader.getDataSet();

		ArffSaver arffSaver = new ArffSaver();
		arffSaver.setInstances(data);
		File outputArffFile = new File(outputArff);
		arffSaver.setFile(outputArffFile);
		arffSaver.writeBatch();
	}
}
