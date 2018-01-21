package com.neu.edu.assign4part4;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text,NullWritable, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(" ");
		if (values[values.length-2].equalsIgnoreCase("404")) {
			context.write(NullWritable.get(), new Text(values[0]));
		}
	}
}
