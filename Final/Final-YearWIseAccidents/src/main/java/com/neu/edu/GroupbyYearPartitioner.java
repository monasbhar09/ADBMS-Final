package com.neu.edu;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupbyYearPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

	private static final String MIN_LAST_DATE_YEAR = "last.year";
	private Configuration conf = null;
	private int minLastAccidentDateYear = 0;

	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() - minLastAccidentDateYear;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		minLastAccidentDateYear = conf.getInt(MIN_LAST_DATE_YEAR, 0);
	}

	public static void setMinLastAccessDate(Job job, int year) {
		job.getConfiguration().setInt(MIN_LAST_DATE_YEAR, year);
	}
}
