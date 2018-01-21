package com.neu.edu.assign6part2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BookMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] separatedInput = value.toString().split(";");
        String isbnNumber = separatedInput[0];
        if (isbnNumber == null) {
            return;
        }
        outkey.set(isbnNumber);
                outvalue.set("A" + value);
        context.write(outkey, outvalue);
    }
}
