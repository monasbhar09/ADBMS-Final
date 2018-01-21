package com.neu.edu.assign2part4;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, MyWritable, NullWritable, Text> {

	private long minValue;
	private String minDate;
	private long maxValue;
	private String maxDate;
	private float maxStockPriceAdjClose;
	private Text result = new Text();

	@Override
	protected void reduce(Text key, Iterable<MyWritable> values, Context context)
			throws IOException, InterruptedException {
		minValue = Long.MAX_VALUE;
		maxValue = Long.MIN_VALUE;
		maxStockPriceAdjClose = Long.MIN_VALUE;
		for (MyWritable value : values) {
			if (minValue > value.getStockVolume()) {
				minValue = value.getStockVolume();
				minDate = value.getDate();
			}
			if (maxValue < value.getStockVolume()) {
				maxValue = value.getStockVolume();
				maxDate = value.getDate();
			}
			if (maxStockPriceAdjClose < value.getMaxAdjClose()) {
				maxStockPriceAdjClose = value.getMaxAdjClose();
			}
		}
		result.set(key.toString()+"-> Max Stock Volume on : " + maxDate + "; Min Stock Volume on " + minDate
				+ "; Max Stock_price_adj_close " + maxStockPriceAdjClose);
		context.write(NullWritable.get(), result);
	}
}