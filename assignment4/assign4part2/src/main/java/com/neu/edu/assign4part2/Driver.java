package com.neu.edu.assign4part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		if (args.length != 2) {
			System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
			System.exit(2);
		}

		conf.setInt(MRJobConfig.COUNTERS_MAX_KEY, 6000);
		Limits.init(conf);

		Path input = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Job job = Job.getInstance(conf, "Counting URL hit using counter");
		job.setJarByClass(Driver.class);

		job.setMapperClass(CountMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		int code = job.waitForCompletion(true) ? 0 : 1;

		if (code == 0) {
			for (Counter counter : job.getCounters().getGroup(CountMapper.IP_ADDRESS_COUNTER)) {
				System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
			}
		}

		FileSystem.get(conf).delete(outputDir, true);
		System.exit(code);
	}
}
