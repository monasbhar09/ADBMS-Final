package com.neu.edu.assign6part3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BookRatingMapper extends Mapper<Object, Text, Text, Text>{
	
	private HashMap<String,String> bookInfo = new HashMap<String, String>(); 
	private Text outValue = new Text();
	
	@SuppressWarnings("resource")
	public void setup(Context context) throws IOException, InterruptedException{
		URI[] files = context.getCacheFiles();
		System.out.println("files="+files);
		for(URI p:files) {
			BufferedReader rdr = new BufferedReader(new FileReader(p.toString()));
			String line = null;
			while((line = rdr.readLine()) != null) {
				String[] fields = line.split(";");
				String isbn = fields[0];
				bookInfo.put(isbn, line);
			}
		}
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String[] fields = value.toString().split(";");
		String isbn = fields[1];
		if(isbn != null && bookInfo.get(isbn) != null) {
			outValue.set(bookInfo.get(isbn));
			context.write(new Text(value.toString()), outValue);
		}
	}

}
