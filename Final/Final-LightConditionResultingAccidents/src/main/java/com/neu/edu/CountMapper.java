package com.neu.edu;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<Object, Text, NullWritable, NullWritable> {

	public static final String LIGHT_CONDITION = "condition";

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		Text t1 = new Text(fields[24]);
		try {
			Integer.parseInt(fields[24]);
		}catch(NumberFormatException e) {
			return;
		}
		if (t1 != null && !t1.toString().isEmpty()) {
			context.getCounter(LIGHT_CONDITION, t1.toString()).increment(1);
		}
	}
}
