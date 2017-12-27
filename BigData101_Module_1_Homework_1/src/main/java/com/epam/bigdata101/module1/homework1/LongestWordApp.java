package com.epam.bigdata101.module1.homework1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LongestWordApp extends Configured implements Tool{
	
    public static void main(String[] args) {
    	int res = -1;
		try {
			res = ToolRunner.run(new Configuration(), new LongestWordApp(), args);
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
        Job job = Job.getInstance(conf);
        job.setJobName("The longest words");
        job.setJarByClass(getClass());
   
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);         
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);   
        job.setSortComparatorClass(DecreasingComparator.class);   //Sorting key in decreasing order
        
        job.setMapperClass(LongestWordMapper.class);
        job.setCombinerClass(LongestWordReducer.class);
        job.setReducerClass(LongestWordReducer.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
