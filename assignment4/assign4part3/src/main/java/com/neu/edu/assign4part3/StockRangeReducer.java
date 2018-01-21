package com.neu.edu.assign4part3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockRangeReducer extends Reducer<Text, Text, Text, Text> {

	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (Text id : values) {
			String keyString = id.toString();
			if(map.containsKey(keyString)) {
				Integer count = new Integer(map.get(keyString).intValue()+1);
				map.put(keyString, count);
			}else {
				map.put(keyString, new Integer(1));
			}
		}
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			sb.append(entry.getKey() + "(" + entry.getValue()+"), ");
		}
		result.set(sb.toString());
		context.write(key, result);
	}

}
