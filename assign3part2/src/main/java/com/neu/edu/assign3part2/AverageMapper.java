package com.neu.edu.assign3part2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {

	private IntWritable year = new IntWritable();
	private CountAverageTuple countAverageTuple = new CountAverageTuple();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		if (value.toString().length() > 80) {
			return;
		} else {
			while (itr.hasMoreTokens()) {
				SimpleDateFormat frmt = new SimpleDateFormat("yyyy-mm-dd");
				try {
					String[] arr = itr.nextToken().split(",");
					String strDate = arr[2];
					Date creationDate = frmt.parse(strDate);
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
					year.set(Integer.parseInt((dateFormat.format(creationDate))));
					countAverageTuple.setAverage(Double.parseDouble(arr[8]));
					countAverageTuple.setCount(1);
					context.write(year, countAverageTuple);
				} catch (ParseException ex) {
					return;
				}
			}
		}
	}
}
