package com.epam.bigdata101.module1.homework2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import eu.bitwalker.useragentutils.UserAgent;

import com.epam.bigdata101.module1.homework2.util.BrowserCounter;
import com.epam.bigdata101.module1.homework2.util.FloatIntTuple;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, FloatIntTuple> {
	
	private Text ipAdress = new Text();
	private FloatIntTuple bytesPerRequest = new FloatIntTuple();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		String line = value.toString();
		
		if (line != null && !line.trim().isEmpty()) { 					// Omitting empty line
			setBrowserCounter(line, context);							// Passing the line to browser detection method
			String[] tokens = value.toString().split(" ");
			ipAdress.set(tokens[0]);									// IP address setting
			if(tokens[9].equals("-")) {bytesPerRequest.set(1, 0);} 		// If numeric value doesn't exists, substituting it with zero
			else {bytesPerRequest.set(1, Integer.parseInt(tokens[9]));}	// Request bytes setting
			
			context.write(ipAdress, bytesPerRequest);
		}
	}
	
	private void setBrowserCounter(String line, Context context) {
		
		//Analyzing user-agent data in line and retrieving browser name	
		String browserName = UserAgent.parseUserAgentString(line).getBrowser().getName();
		
		//Setting browser name in appropriate counter
		if(browserName.contains("Apple WebKit")) {
			context.getCounter(BrowserCounter.APPLE_WEBKIT).increment(1);
		}
		if(browserName.contains("Chrome")) {
			context.getCounter(BrowserCounter.GOOGLE_CHROME).increment(1);
		}
		if(browserName.contains("Explorer")) {
			context.getCounter(BrowserCounter.INTERNET_EXPLORER).increment(1);
		}
		if(browserName.contains("Konqueror")) {
			context.getCounter(BrowserCounter.KONQUEROR).increment(1);
		}
		if(browserName.contains("Mozilla") || browserName.contains("Firefox")){
			context.getCounter(BrowserCounter.MOZILLA_FIREFOX).increment(1);
		}
		if(browserName.contains("Opera")){
			context.getCounter(BrowserCounter.OPERA).increment(1);
		}
		if(browserName.contains("Safari")) {
			context.getCounter(BrowserCounter.SAFARI).increment(1);
		}
		if(browserName.contains("SeaMonkey")) {
			context.getCounter(BrowserCounter.SEAMONKEY).increment(1);
		}
	}
}
