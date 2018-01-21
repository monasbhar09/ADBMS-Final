package com.neu.edu.assign5part3;

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
		String[] row = value.toString().split(" ");
		String httpRequest = row[5];

		httpRequest = httpRequest.substring(1);
		if (httpRequest.equalsIgnoreCase("GET")) {
			multipleOutput.write("bins", value, NullWritable.get(), "GET");
		}
		if (httpRequest.equalsIgnoreCase("POST")) {
			multipleOutput.write("bins", value, NullWritable.get(), "POST");
		}
		if (httpRequest.equalsIgnoreCase("HEAD")) {
			multipleOutput.write("bins", value, NullWritable.get(), "HEAD");
		}
		if (httpRequest.equalsIgnoreCase("PUT")) {
			multipleOutput.write("bins", value, NullWritable.get(), "PUT");
		}
		if (httpRequest.equalsIgnoreCase("DELETE")) {
			multipleOutput.write("bins", value, NullWritable.get(), "DELETE");
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutput.close();
	}

}
