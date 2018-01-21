package com.neu.edu.assign2part5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path intermediateOutputDir = new Path(args[1]);
		Path outputDir = new Path(args[2]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = Job.getInstance(conf, "AverageRateCalculation");
		job.setJarByClass(AverageMapper.class);

		// Setup MapReduce
		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);

		// Specify key / value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, intermediateOutputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(intermediateOutputDir))
			hdfs.delete(intermediateOutputDir, true);
		
		boolean complete = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "SecondarySorting");

		if (complete) {
			// Setup MapReduce
			job2.setJarByClass(SortingMapper.class);
			job2.setMapperClass(SortingMapper.class);
			job2.setReducerClass(SortingReducer.class);

			// Specify key / value
			job2.setOutputKeyClass(CustomWritable.class);
			job2.setOutputValueClass(IntWritable.class);

			// Input
			FileInputFormat.addInputPath(job2, intermediateOutputDir);
			FileOutputFormat.setOutputPath(job2, outputDir);
			
			if (hdfs.exists(outputDir))
				hdfs.delete(outputDir, true);
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}

}
