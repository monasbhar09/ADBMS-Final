package com.neu.edu.assign3part3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevWritable> {

	private MedianStdDevWritable result = new MedianStdDevWritable();
	private List<Float> list = new ArrayList<Float>();

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		float count = 0;
		list.clear();
		result.setStandardDeviation(0);

		for (IntWritable val : values) {
			list.add((float) val.get());
			sum += val.get();
			++count;
		}

		Collections.sort(list);

		if (count % 2 == 0) {
			result.setMedian((list.get((int) count / 2 - 1) + list.get((int) count / 2)) / 2.0f);

		} else {
			result.setMedian(list.get((int) count / 2));
		}

		float mean = sum / count;
		float sumOfSquares = 0.0f;

		for (Float val : list) {
			sumOfSquares += (val - mean) * (val - mean);
		}
		result.setStandardDeviation((float) Math.sqrt(sumOfSquares / (count - 1)));
		context.write(key, result);
	}
}