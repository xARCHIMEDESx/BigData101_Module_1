package com.epam.bigdata101.module1.homework3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.epam.bigdata101.module1.homework3.util.StringIntTuple;

public class BiddingDatasetReducer extends Reducer<StringIntTuple, IntWritable, Text, IntWritable> {
	
	private IntWritable eventCount = new IntWritable();
	private Text city = new Text();
		
	@Override
	public void reduce(StringIntTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		int count = 0;
	    for (IntWritable val : values) {
	    	count += val.get();
        }			    
	    eventCount.set(count);
	    city.set(key.getLeft());            // Writing only the part of key, which contains city name
		context.write(city, eventCount);	
	}
}

