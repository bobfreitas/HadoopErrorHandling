package com.freitas.hadoop.counter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * This class provides an example of tracking any number not-fatal errors. 
 * These are errors that you want to keep track of but do not require 
 * immediate action and can be analysed at a later time.  
 */
public class WordCountWithErrorCount extends Configured implements Tool {
	
	private static final String COUNTER_GROUP = "WordCountErrors";
	private static final String COUNTER = "BadWords";

	public static class TokenizerMapper 
		extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static Map<String,String> badWords = new HashMap<String,String>();
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			badWords.put("bad", "bad");
			badWords.put("black", "black");
			badWords.put("angry", "angry");
		}

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if (badWords.get(word.toString().toLowerCase()) != null){
					context.getCounter(COUNTER_GROUP, COUNTER).increment(1);
				}
				else {
					context.write(word, one);
				}
			}
		}
	}

	public static class IntSumReducer 
		extends Reducer<Text,IntWritable,Text,IntWritable> {
		
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
	
	

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String path = "/etc/hadoop/conf";
		conf.addResource(new Path(path + "/core-site.xml"));
		conf.addResource(new Path(path + "/hdfs-site.xml"));
		conf.addResource(new Path(path + "/mapred-site.xml"));
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountWithErrorCount.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// want to delete output dir first
		FileSystem fs = FileSystem.get(conf);
		Path outDir = new Path(args[1]);
		fs.delete(outDir,true);
		FileOutputFormat.setOutputPath(job, outDir);
		
		boolean okay = job.waitForCompletion(true);
		if (okay){
			Counters counters = job.getCounters();
			Counter bwc = counters.findCounter(COUNTER_GROUP, COUNTER);
			System.out.println();
			System.out.println("Errors detected, " + bwc.getDisplayName()+":" + bwc.getValue());
			System.out.println();
		}
		
		return (okay ? 0 : 1);
	}
	
	
	public static void main(String[] args) throws Exception {
		args = new String[] { "/user/kitenga/wordcount/in/", "/user/kitenga/wordcount/out2" };
		ToolRunner.run(new WordCountWithErrorCount(), args);
	}

}
