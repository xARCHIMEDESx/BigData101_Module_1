package com.epam.bigdata101.module1.homework2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.epam.bigdata101.module1.homework2.util.FloatIntTuple;

public class WebLogReducer extends Reducer<Text, FloatIntTuple, Text, FloatIntTuple> {

	private FloatIntTuple averageAndTotalBytesTuple = new FloatIntTuple();
		
	@Override
	public void reduce(Text key, Iterable<FloatIntTuple> values, Context context) throws IOException, InterruptedException {		
		float averageBytes;
		int totalBytes = 0;
	    float count = 0f;
	    
	    for (FloatIntTuple val : values) {
	    	totalBytes += val.getRight();
	    	count += val.getLeft();
        }     
		averageBytes = totalBytes/count;							// Dividing the sum of bytes per IP by number of requests
		averageAndTotalBytesTuple.set(averageBytes, totalBytes);
		
		context.write(key, averageAndTotalBytesTuple);	
	}
}

