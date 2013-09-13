package com.freitas.hadoop.tasktracker;

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
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.freitas.util.ErrMsgHelper;

/*
 * This class provides an example of reching into the TaskTracker to get 
 * additional information on the status of a job to expose why it failed.  
 */
public class WordCountTaskTracker extends Configured implements Tool {
	
	public static class TokenizerMapper 
		extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			if (key != null){
				throw new IOException("Some funky error");
			}
			
			StringTokenizer itr = new StringTokenizer(value.toString());
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
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String path = "/etc/hadoop/conf";
		conf.addResource(new Path(path + "/core-site.xml"));
		conf.addResource(new Path(path + "/hdfs-site.xml"));
		conf.addResource(new Path(path + "/mapred-site.xml"));
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCountTaskTracker.class);
		
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
		
		System.out.println("\n");
		JobID jobId = job.getJobID();
		System.out.println("job id: " + jobId.toString());
		
		InetSocketAddress inetAddr = JobTracker.getAddress(conf);
		JobClient jobClient = new JobClient(inetAddr, conf);
		
		org.apache.hadoop.mapred.JobID oldJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
		RunningJob runningJob = jobClient.getJob(oldJobId);
		JobStatus jobStatus = runningJob.getJobStatus();
		System.out.println("Job status: " + JobStatus.getJobRunState(jobStatus.getRunState()));
		
		String errMsg = null;
		if (jobStatus.getRunState() != JobStatus.SUCCEEDED){
			errMsg = ErrMsgHelper.getCleanError(jobClient, oldJobId);
			System.out.println("Error msg: " + errMsg);
		}
		
		return (okay ? 0 : 1);
	}
	
	
	public static void main(String[] args) throws Exception {
		args = new String[] { "/user/kitenga/bio/pubmed1.txt", "/user/kitenga/wordcount/out2" };
		ToolRunner.run(new WordCountTaskTracker(), args);
		
	}

}
