package com.neu.edu.Final_BinningPattern;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> multipleOutput = null;

	protected void setup(Context context) {
		multipleOutput = new MultipleOutputs<Text, NullWritable>(context);
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		try {
			String year = fields[9].split("/")[2];
			multipleOutput.write("bins", value, NullWritable.get(), year);
		} catch (Exception e) {
			return;
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutput.close();
	}

}
