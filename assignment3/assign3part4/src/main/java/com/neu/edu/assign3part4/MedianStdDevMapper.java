package com.neu.edu.assign3part4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStdDevMapper extends Mapper<LongWritable, Text, LongWritable, SortedMapWritable> {

	Long movieId = 0L;
	LongWritable count = new LongWritable(1);
	SortedMapWritable cwo = new SortedMapWritable();
	DoubleWritable rating = new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] str = value.toString().split("::");
		movieId = Long.parseLong(str[1]);
		rating.set(Double.parseDouble(str[2]));

		cwo.put(rating, count);
		context.write(new LongWritable(movieId), cwo);
	}
}