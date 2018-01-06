package com.epam.bigdata101.module1.homework3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import eu.bitwalker.useragentutils.UserAgent;

import com.epam.bigdata101.module1.homework3.util.StringIntTuple;
import com.epam.bigdata101.module1.homework3.util.OperatingSystemName;

public class BiddingDatasetMapper extends Mapper<LongWritable, Text, StringIntTuple, IntWritable> {
	
	private StringIntTuple cityName = new StringIntTuple();
	private IntWritable one = new IntWritable(1);
	private Map<Integer, String> cities = new HashMap<>();
	private Map<Integer, String> regions = new HashMap<>();
	
	protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
    	URI[] files = context.getCacheFiles();
    	DistributedCacheLoading(files);       		    	  
    }

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();			
		if (line != null && !line.trim().isEmpty()) {				
			String[] tokens = value.toString().split("\t");			
			int bidPrice = Integer.valueOf(tokens[19]);
			int cityID = Integer.valueOf(tokens[7]);
			int os;
			
			if(bidPrice > 250) {							// Filtering bids with price > 250	
				os = operatingSystemNameDetection(line);	// Passing the line to OS detection method and attaching the result to the key			
				if (cities.containsKey(cityID)) {				
					cityName.set(cities.get(cityID), os);
				} else if (regions.containsKey(cityID)) {
					cityName.set(regions.get(cityID), os);	
				} else {
					// If city ID is not present in cities or regions lists, declaring it a missing record
					cityName.set(("MISSING CITY ID "+ cityID), os);	  
				}				
				context.write(cityName, one);	
			}											
		}
	}	
	
	/* Analyzing the input line and detecting OS name.
	   Since the majority of request were done from Windows XP (approx. 60%), it was decided to use two reducers:
	   one receives records with WinXP OS, another - with others. According to OS, method returns value which is passed to partitioner later */
	private int operatingSystemNameDetection(String line) {
		String osName = UserAgent.parseUserAgentString(line).getOperatingSystem().getName();
		return osName.contains("Windows XP") ? OperatingSystemName.WINDOWS_XP.getCode() : OperatingSystemName.OTHER.getCode();
	}
		
	private void DistributedCacheLoading(URI[] files) throws IOException {
		Path path1 = new Path(files[0]);
    	Path path2 = new Path(files[1]);
    	Path citiesFilePath;
		Path regionsFilePath;
    		
		// Loading the distributed cache content to HashMaps for further usage.	
    	if(path1.getName().equals("city.en.txt") && path2.getName().equals("region.en.txt")) {
    		citiesFilePath = path1;
    		regionsFilePath = path2;
    	} else if (path1.getName().equals("region.en.txt") && path2.getName().equals("city.en.txt")){
    		regionsFilePath = path1;
    		citiesFilePath = path2;    		
    	} else {throw new IOException("Error! Wrong file name in distributed cache path");}
    	    	
    	try(BufferedReader reader = new BufferedReader(new FileReader(citiesFilePath.getName()))) {
        	String line = "";
        	while ((line = reader.readLine()) != null) {
        		String city[] = line.split("\t");
        		cities.put(Integer.valueOf(city[0]), city[1]);
        	}
    	}    	
    	try(BufferedReader reader = new BufferedReader(new FileReader(regionsFilePath.getName()))) {
        	String line = "";
        	while ((line = reader.readLine()) != null) {
        		String region[] = line.split("\t");
        		regions.put(Integer.valueOf(region[0]), region[1]);
        	}
    	}
	}
}
