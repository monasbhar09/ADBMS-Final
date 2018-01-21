package com.neu.edu.Final_InnerJoinAccidentsCasualties;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CasualtyMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] separatedInput = value.toString().split(",");

        String accidentIndex = separatedInput[0];
        if (accidentIndex == null) {
            return;
        }
        outkey.set(accidentIndex);
        outvalue.set("C" + value);
        context.write(outkey, outvalue);
    }
}
