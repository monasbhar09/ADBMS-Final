package com.neu.edu.assign2part3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);
		FixedLengthInputFormat.setRecordLength(conf, 1);
		// Create job
		Job job = Job.getInstance(conf, "FixedLengthInputFormat");
		job.setJarByClass(FixedLengthMapper.class);
		
		FixedLengthInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
		// Setup MapReduce
		job.setMapperClass(FixedLengthMapper.class);
		job.setReducerClass(FixedLengthReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(FixedLengthInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setJobName("Fixed Record Test");

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

}