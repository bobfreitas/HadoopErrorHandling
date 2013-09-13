package com.freitas.hadoop.counter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * This class provides an example of handling a catastrophic a error that
 * forces you to end the job, because there is no chance of it succeeding.  
 * This would happen when there is some critical dependency that the job
 * requires to proceed.  You would detect the error in the setup() and 
 * return a counter that can then be converted into a distinct error.  
 */
public class WordCountWithEnum extends Configured implements Tool {
	
	public static class TokenizerMapper 
		extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private int needToStop = 0;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				String toFail = returnNull();
				toFail.toString();
			}
			catch (NullPointerException npe){
				npe.printStackTrace();
				context.getCounter(ExceptionTypeEnum.PLACEHOLDER).setValue(
						ExceptionTypeEnum.NULL_POINTER.getErrorCode());
				needToStop++;
				// don't throw an exception here, it will zero out the counter
			}
		}

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			// need to make sure do the map at least once so the 
			// counter will be saved in the Hadoop framework
			if (needToStop > 0){
				if (needToStop > 1){
					killJob(context);
				}
				needToStop++;
				return;
			}
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
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
	
	
	private static String returnNull(){
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	private static void killJob(Context context) throws IOException{
		JobID jobId = context.getJobID();
		InetSocketAddress inetAddr = JobTracker.getAddress(context.getConfiguration());
		JobClient jobClient = new JobClient(inetAddr, context.getConfiguration());
		RunningJob runningJob = jobClient.getJob((org.apache.hadoop.mapred.JobID) jobId);
		runningJob.killJob();
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String path = "/etc/hadoop/conf";
		conf.addResource(new Path(path + "/core-site.xml"));
		conf.addResource(new Path(path + "/hdfs-site.xml"));
		conf.addResource(new Path(path + "/mapred-site.xml"));
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountWithEnum.class);
		
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
		
		Counters counters = job.getCounters();
		Counter bwc = counters.findCounter(ExceptionTypeEnum.PLACEHOLDER);
		// use the value of the counter to lookup the error
		ExceptionTypeEnum err = ExceptionTypeEnum.get(bwc.getValue());
		System.out.println();
		System.out.println("Terminal error, " + err.getName());
		System.out.println("With message: " + err.getMessage());
		System.out.println();
		
		return (okay ? 0 : 1);
	}
	
	
	public static void main(String[] args) throws Exception {
		args = new String[] { "/user/kitenga/bio/pubmed1.txt", "/user/kitenga/wordcount/out2" };
		ToolRunner.run(new WordCountWithEnum(), args);
	}

}
