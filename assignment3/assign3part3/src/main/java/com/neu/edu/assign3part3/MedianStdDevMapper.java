package com.neu.edu.assign3part3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	IntWritable movieId = new IntWritable();
	IntWritable rating = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String arr[] = value.toString().split("::");
		int movie = Integer.parseInt(arr[1]);
		int rate = Integer.parseInt(arr[2]);
		movieId.set(movie);
		rating.set(rate);
		context.write(movieId, rating);
	}
}
