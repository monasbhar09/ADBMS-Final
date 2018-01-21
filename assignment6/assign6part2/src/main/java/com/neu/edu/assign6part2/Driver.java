package com.neu.edu.assign6part2;

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

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BookMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileSystem hdfs = FileSystem.get(conf);
        
		if (hdfs.exists(new Path(args[3])))
			hdfs.delete(new Path(args[3]), true);
		
        boolean complete = job.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        
        Job job2 = Job.getInstance(conf2, "ReduceSideJoinChaining");
        if (complete) {
            job2.setJarByClass(JoinReducer.class);
            MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, UserMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, BookAndRatingMapper.class);
            job2.setReducerClass(JoinReducer.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job2, new Path(args[4]));
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            
    		if (hdfs.exists(new Path(args[4])))
    			hdfs.delete(new Path(args[4]), true);
    		
            complete = job2.waitForCompletion(true);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }

}
