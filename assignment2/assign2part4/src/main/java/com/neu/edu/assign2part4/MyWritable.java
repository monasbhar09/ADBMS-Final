package com.neu.edu.assign2part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class MyWritable implements Writable, WritableComparable<MyWritable>{

    private String date;
    private long stockVolume;
    private float maxAdjClose;
    
    
    public MyWritable() {
		// TODO Auto-generated constructor stub
	}

    public MyWritable(String date, long stockVolume, float maxAdjClose) {
        this.date = date;
        this.stockVolume = stockVolume;
        this.maxAdjClose = maxAdjClose;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getStockVolume() {
        return stockVolume;
    }

    public void setStockVolume(long stockVolume) {
        this.stockVolume = stockVolume;
    }

    public float getMaxAdjClose() {
        return maxAdjClose;
    }

    public void setMaxAdjClose(float maxAdjClose) {
        this.maxAdjClose = maxAdjClose;
    }
   
    
    @Override
    public void write(DataOutput d) throws IOException {
        WritableUtils.writeString(d, date);
        d.writeLong(stockVolume);
        d.writeFloat(maxAdjClose);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        date = WritableUtils.readString(di);
        stockVolume = di.readLong();
        maxAdjClose = di.readFloat();
    }

    @Override
    public int compareTo(MyWritable o) {
       return -1*(date.compareTo(o.date));
    }  
    
}
