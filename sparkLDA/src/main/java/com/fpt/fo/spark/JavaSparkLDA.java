package com.fpt.fo.spark;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class JavaSparkLDA {
	public static void main(String[] args) throws IOException {
		// String path = "./data/docs/*.md";
		String path = "./data/docsvn/*.txt";

		SparkConf conf = new SparkConf().setAppName("SparkLDA").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// JavaPairRDD<String, String> text = sc.wholeTextFiles(path);
		JavaRDD<String> corpus = sc.wholeTextFiles(path).map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> x) {
				return x._2();
			}
		});

		JavaRDD<List<String>> tokenized = corpus.map(new Function<String, List<String>>() {
			public List<String> call(String x) {
				List<String> words = Arrays.asList(x.toLowerCase().split("\\s"));
				List<String> wordsFilter = new ArrayList<>();
				for (String word : words) {
					if (word.length() <= 3 || word.matches(".*\\d+.*") || word.equals("\n") || word.contains(".")) {
						continue;
					} else {
						wordsFilter.add(word);
					}
				}
				return wordsFilter;
			}
		});

		JavaRDD<String> words = tokenized.flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> x) {
				return x;
			}
		});

		List<Tuple2<String, Long>> termCounts = words.mapToPair(new PairFunction<String, String, Long>() {
			public Tuple2<String, Long> call(String x) {
				return new Tuple2<String, Long>(x, 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			public Long call(Long x, Long y) {
				return x + y;
			}
		}).collect();

		Collections.sort(termCounts, new Comparator<Tuple2<String, Long>>() {
			public int compare(Tuple2<String, Long> x, Tuple2<String, Long> y) {
				return y._2().compareTo(x._2());
			}
		});

		int numStopwords = 20;
		for (int i = 0; i < 21; i++) {
			termCounts.remove(i);
		}

		List<String> vocabArray = new ArrayList<>();
		for (Tuple2<String, Long> term : termCounts) {
			vocabArray.add(term._1());
		}

		Map<String, Integer> vocab = new HashMap<>();
		Map<Integer, String> vocab2 = new HashMap<>();
		int i = 0;
		for (String word : vocabArray) {
			vocab.put(word, i);
			vocab2.put(i, word);
			i++;
		}

		JavaRDD<Vector> parsedData = tokenized.map(new Function<List<String>, Vector>() {
			public Vector call(List<String> x) {
				List<Integer> listInInt = new ArrayList<>();
				for (String word : x) {
					Integer index;
					index = vocab.get(word);
					if (index == null) {
						continue;
					} else {
						listInInt.add(vocab.get(word));
					}
				}
				Integer[] s = listInInt.toArray(new Integer[listInInt.size()]);
				double[] values = new double[s.length];
				for (int i = 0; i < s.length; i++) {
					values[i] = (double) s[i];
				}
				return Vectors.dense(values);
			}
		});

//		parsedData.saveAsTextFile("./data/java/parsedData2");

		JavaPairRDD<Long, Vector> corpus2 = JavaPairRDD
				.fromJavaRDD(parsedData.zipWithIndex().map(new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
					public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
						return doc_id.swap();
					}
				}));
		corpus2.cache();
//		corpus2.saveAsTextFile("./data/java/corpus2");
		
		LDAModel ldaModel = new LDA().setK(4).run(corpus2);
		
		Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(10);
		PrintWriter pr = new PrintWriter(new FileWriter("./data/javaLDA"));
		for(Tuple2<int[], double[]> topicIndice : topicIndices){
			pr.println("TOPIC: ");
			for(int j = 0; j<topicIndice._1().length;j++){
				pr.println(vocab2.get(topicIndice._1()[j]) + " | " + topicIndice._2()[j]);
			}
		}
		pr.close();
		sc.close();

	}
}
