package com.neu.edu.Final_InnerJoinAccidentsCasualties;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
	private Text tmp = new Text();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		listA.clear();
		listB.clear();
		while (values.iterator().hasNext()) {
			tmp = values.iterator().next();
			if ((Character.toString((char) tmp.charAt(0)).equals("A"))) {
				listA.add(new Text(tmp.toString().substring(1)));
			}
			if ((Character.toString((char) tmp.charAt(0)).equals("C"))) {
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}
		executeJoinLogic(context);
	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException {

		if (!listA.isEmpty() && !listB.isEmpty()) {
			for (Text A : listA) {
				for (Text B : listB) {
					context.write(A, B);
				}
			}
		}

	}

}
