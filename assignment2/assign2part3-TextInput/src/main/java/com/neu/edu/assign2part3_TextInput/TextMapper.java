package com.neu.edu.assign2part3_TextInput;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TextMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
	       
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	               
	           
	            String[] fields = value.toString().split(" ");
	            if(fields[0].contains("[")){
	                Text t1 = new Text(fields[5]);
	                context.write(t1, one);
	            }
		
	        }
}