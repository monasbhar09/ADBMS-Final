package com.neu.edu.assign2part5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducer extends Reducer<CustomWritable, IntWritable, CustomWritable, IntWritable> {

    public static int count = 1;

    @SuppressWarnings("unused")
	@Override
    protected void reduce(CustomWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable val : values) {
            if (count <= 25) {
                context.write(key, new IntWritable(count));
                count++;
            } else {
                break;
            }
        }
    }
}
