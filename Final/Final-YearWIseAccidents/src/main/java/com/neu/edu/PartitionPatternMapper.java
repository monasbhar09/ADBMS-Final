package com.neu.edu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionPatternMapper extends Mapper<Object, Text, IntWritable, Text> {

	private IntWritable outputKey = new IntWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] row = value.toString().split(",");
		try {
			int year = Integer.parseInt(row[9].toString().split("/")[2]);
			outputKey.set(year);
			context.write(outputKey, value);
		} catch (Exception e) {
			return;
		}
	}
}
