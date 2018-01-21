package com.neu.edu.assign2part4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, MyWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String values[] = value.toString().split(",");
		if (values.length < 8)
			return;
		try {
			MyWritable myData = new MyWritable(values[2], Long.parseLong(values[7]),
					Float.parseFloat(values[8]));
			context.write(new Text(values[1]), myData);
		} catch (Exception e) {
			return;
		}
	}

}
