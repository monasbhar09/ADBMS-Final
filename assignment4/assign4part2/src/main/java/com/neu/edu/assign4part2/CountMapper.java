package com.neu.edu.assign4part2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

	public static final String IP_ADDRESS_COUNTER = "IP Address";

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(" ");
		Text t1 = new Text(fields[0]);
		if (t1 != null && !t1.toString().isEmpty()) {
			context.getCounter(IP_ADDRESS_COUNTER, t1.toString()).increment(1);
		}
	}
}
