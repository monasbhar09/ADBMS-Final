package com.neu.edu.assign4part3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockRangeMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String values[] = value.toString().split(",");

		Float stockPrice = 0.0f;
		String stockName = "";
		String range = "";
		try {
			stockName = values[1];
			stockPrice = Float.parseFloat(values[3]);
			Integer temp = 0;
			temp = (int) (stockPrice / 10);
			range = (temp * 10 + "-" + (temp + 1) * 10);
			context.write(new Text(range), new Text(stockName));
		} catch (Exception e) {
			return;
		}

	}
}
