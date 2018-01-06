package com.epam.bigdata101.module1.homework2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.epam.bigdata101.module1.homework2.util.FloatIntTuple;

public class WebLogCombiner extends Reducer<Text, FloatIntTuple, Text, FloatIntTuple> {

	private FloatIntTuple countAndTotalBytesTuple = new FloatIntTuple();
		
	@Override
	public void reduce(Text key, Iterable<FloatIntTuple> values, Context context) throws IOException, InterruptedException {		
		int totalBytes = 0;
	    int count = 0;
	    
	    for (FloatIntTuple val : values) {
	    	totalBytes += val.getRight();
	    	count++;
        }     						
		countAndTotalBytesTuple.set(count, totalBytes);   // passing to reducer aggregated bytes value and number of that aggregations
		
		context.write(key, countAndTotalBytesTuple);	
	}
}