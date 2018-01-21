package com.neu.edu.assign4part5;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctFilterMapper extends Mapper<Object, Text, Text, NullWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(" ");
		context.write(new Text(values[0]), NullWritable.get());
	}
}
