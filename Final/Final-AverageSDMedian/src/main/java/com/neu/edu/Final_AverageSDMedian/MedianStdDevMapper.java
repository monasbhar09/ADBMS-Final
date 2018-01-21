package com.neu.edu.Final_AverageSDMedian;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianStdDevMapper extends Mapper<Object, Text, Text, IntWritable> {

	Text dateKey = new Text();
	IntWritable casualtiesValue = new IntWritable();
	DateFormat inputFormat = new SimpleDateFormat("dd/MM/yyyy");
	DateFormat outputFormat = new SimpleDateFormat("yyyy/MM/dd");

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		try {
			Date dateInput = (Date) inputFormat.parse(fields[9]);
			String dateOutput = outputFormat.format(dateInput);
			int casualties = Integer.parseInt(fields[8]);
			dateKey.set(dateOutput);
			casualtiesValue.set(casualties);
			context.write(dateKey, casualtiesValue);
		} catch (Exception e) {
			return;
		}
	}
}
