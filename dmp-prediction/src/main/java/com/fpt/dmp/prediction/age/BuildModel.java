package com.fpt.dmp.prediction.age;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import weka.classifiers.Evaluation;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;

public class BuildModel {

	private static String fileCsv;
	private static String fileArff;
	private static String fileModel;

	public static void main(String[] args) throws Exception {
		System.out.println("START...");

		fileCsv = "data/train_smote_balance.csv";
		fileArff = "data/train_smote_balance.arff";
		fileModel = "data/svm.model";

		CsvToArff(fileCsv, fileArff);

		BufferedReader dataFile = readDataFile(fileArff);
		Instances data = new Instances(dataFile);
		data.setClassIndex(0);

//		trainRandomForest(data, fileModel);
		trainSVM(data, fileModel);
		System.out.println("DONE");
	}

	

	public static void trainLinearRegression(Instances trainData, String outputModel) throws Exception {
		LinearRegression linearRegression = new LinearRegression();
		linearRegression.buildClassifier(trainData);
	}

	public static void trainRandomForest(Instances trainData, String outputModel) throws Exception {
		Integer numTrees = 100;
		Integer numFeatures = 5;

		System.out.println("-----RANDOM FOREST");
		RandomForest randomForest = new RandomForest();
		randomForest.setNumTrees(numTrees);
		randomForest.setNumFeatures(numFeatures);

		Integer numFolds = 10;
		Evaluation evaluation = new Evaluation(trainData);
		evaluation.crossValidateModel(randomForest, trainData, numFolds, new Random(1));
		System.out.println(evaluation.toSummaryString());
		System.out.println(evaluation.toClassDetailsString());

		randomForest.buildClassifier(trainData);
		SerializationHelper.write(outputModel, randomForest);
		System.out.println("Saved trained model to " + outputModel);
	}
	
	public static void trainSVM(Instances trainData, String outputModel) throws Exception {
		System.out.println("-----SVM");
		LibSVM svm = new LibSVM();
		
		Integer numFolds = 10;
		Evaluation evaluation = new Evaluation(trainData);
		evaluation.crossValidateModel(svm, trainData, numFolds, new Random(1));
		System.out.println(evaluation.toSummaryString());
		System.out.println(evaluation.toClassDetailsString());
		
		svm.buildClassifier(trainData);
		SerializationHelper.write(outputModel, svm);
		System.out.println("Saved trained model to " + outputModel);
	}

	public static void CsvToArff(String inputCsv, String outputArff) throws IOException {
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File(inputCsv));
		Instances data = loader.getDataSet();

		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		File outputArffFile = new File(outputArff);
		saver.setFile(outputArffFile);
		saver.writeBatch();
	}

	public static BufferedReader readDataFile(String filename) {
		BufferedReader inputReader = null;

		try {
			inputReader = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException ex) {
			System.err.println("File not found: " + filename);
		}

		return inputReader;
	}
}
