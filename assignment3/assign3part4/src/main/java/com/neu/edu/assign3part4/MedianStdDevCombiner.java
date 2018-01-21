package com.neu.edu.assign3part4;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevCombiner extends Reducer<LongWritable, SortedMapWritable, LongWritable, SortedMapWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<SortedMapWritable> value, Context context)
			throws IOException, InterruptedException {

		SortedMapWritable outval = new SortedMapWritable();
		for (SortedMapWritable v : value) {
			for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
				LongWritable count = (LongWritable) outval.get(entry.getKey());

				if (count != null) {
					count.set(count.get() + ((LongWritable) entry.getValue()).get());
				} else {
					outval.put(entry.getKey(), new LongWritable(((LongWritable) entry.getValue()).get()));
				}
			}
			v.clear();
		}
		context.write(key, outval);
	}
}
