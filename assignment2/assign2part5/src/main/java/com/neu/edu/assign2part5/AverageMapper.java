package com.neu.edu.assign2part5;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        LongWritable movie = new LongWritable();
        FloatWritable rating = new FloatWritable();
        String str[] = value.toString().split("::");
        movie.set(Long.parseLong(str[1]));
        rating.set(Float.parseFloat(str[2]));
        context.write(movie, rating);
    }

}

