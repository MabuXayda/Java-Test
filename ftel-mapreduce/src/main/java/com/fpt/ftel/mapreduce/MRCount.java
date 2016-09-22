package com.fpt.ftel.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
			if (parts.length > 5 && parts[5].length() == 16) {
				word.set(parts[5]);
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void countCustomerId(List<String> listFilePath, String fileOutPut) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "user id count");
		job.setJarByClass(MRCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		StringBuilder allFilePath = new StringBuilder();
		String prefix = "";
		for (String filePath : listFilePath) {
			allFilePath.append(prefix);
			prefix = ",";
			allFilePath.append(filePath);
		}
		FileInputFormat.addInputPaths(job, allFilePath.toString());
		FileOutputFormat.setOutputPath(job, new Path(fileOutPut));
		job.waitForCompletion(true);
		while (!job.isComplete()) {
		}
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}	
