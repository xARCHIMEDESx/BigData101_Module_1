package com.epam.bigdata101.module1.homework3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.epam.bigdata101.module1.homework3.util.StringIntTuple;

public class BiddingDatasetApp extends Configured implements Tool{
	
    public static void main(String[] args) {
        int res = -1;
		try {
			res = ToolRunner.run(new Configuration(), new BiddingDatasetApp(), args);
		} catch (Exception e) {
			e.printStackTrace();
		} finally { 
			System.exit(res);
		}                       
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
        	 throw new IOException("usage: [input] [output]");
        }
        
    	Configuration conf = getConf();
    	conf.set(TextOutputFormat.SEPERATOR, "-->");
        Job job = Job.getInstance(conf);
        job.setJobName("Bidding Dataset");
        job.setJarByClass(getClass());
   
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.addCacheFile(new Path("/user/maria_dev/BigData101/Module1/Homework3/DistributedCacheData/city.en.txt").toUri());
        job.addCacheFile(new Path("/user/maria_dev/BigData101/Module1/Homework3/DistributedCacheData/region.en.txt").toUri());
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(StringIntTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class); 
        
        job.setMapperClass(BiddingDatasetMapper.class);
        job.setCombinerClass(BiddingDatasetCombiner.class);
        job.setPartitionerClass(BiddingDatasetPartitioner.class);
        job.setReducerClass(BiddingDatasetReducer.class);
        job.setNumReduceTasks(2);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}