package com.epam.bigdata101.module1.homework3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.epam.bigdata101.module1.homework3.util.StringIntTuple;

public class BiddingDatasetCombiner extends Reducer<StringIntTuple, IntWritable, StringIntTuple, IntWritable> {
	
	private IntWritable eventCount = new IntWritable();
		
	@Override
	public void reduce(StringIntTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		int count = 0;
	    for (IntWritable val : values) {
	    	count += val.get();
        }			    
	    eventCount.set(count);
		context.write(key, eventCount);	
	}
}

