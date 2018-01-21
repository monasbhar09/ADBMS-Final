package com.neu.edu.Final_InnerJoinAccidentsCasualties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ReduceSideJoin");
		job.setJarByClass(JoinReducer.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AccidentMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CasualtyMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(new Path(args[2])))
			hdfs.delete(new Path(args[2]), true);

		job.waitForCompletion(true);

	}

}
