package com.neu.edu.assign2part3_sequence;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SequenceMapper extends Mapper<Text, Text, Text, Text> {

	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}
