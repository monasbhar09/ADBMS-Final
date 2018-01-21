package com.neu.edu.assign2part3;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FixedLengthMapper extends Mapper<LongWritable, BytesWritable, LongWritable, BytesWritable> {

	public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}

}