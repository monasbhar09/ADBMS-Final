package com.neu.edu.assign5part2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionPatternMapper extends Mapper<Object, Text, IntWritable, Text> {

	private IntWritable outputKey = new IntWritable();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        String[] row = value.toString().split(" ");
        String timeStamp = row[3];
        String date = timeStamp.substring(1, timeStamp.length()).trim();
        String[] arr = date.toString().split("/");

        String month = arr[1];

        if (month.trim().equals("Jan")) {
            outputKey.set(1);
        }
        else if (month.trim().equals("Feb")) {
            outputKey.set(2);
        }
        else if (month.trim().equals("Mar")) {
            outputKey.set(3);
        }
        else if (month.trim().equals("Apr")) {
            outputKey.set(4);
        }
        else if (month.trim().equals("May")) {
            outputKey.set(5);
        }
        else if (month.trim().equals("Jun")) {
            outputKey.set(6);
        }
        else if (month.trim().equals("Jul")) {
            outputKey.set(7);
        }
        else if (month.trim().equals("Aug")) {
            outputKey.set(8);
        }
        else if (month.trim().equals("Sep")) {
            outputKey.set(9);
        }
        else if (month.trim().equals("Oct")) {
            outputKey.set(10);
        }
        else if (month.trim().equals("Nov")) {
            outputKey.set(11);
        }
        else if (month.trim().equals("Dec")) {
            outputKey.set(12);
        } 

        context.write(outputKey, value);
    }
}
