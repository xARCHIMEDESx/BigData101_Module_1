package com.epam.bigdata101.module1.homework2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.epam.bigdata101.module1.homework2.util.FloatIntTuple;

public class WebLogApp extends Configured implements Tool{
	
    public static void main(String[] args) {
        int res = -1;
		try {
			res = ToolRunner.run(new Configuration(), new WebLogApp(), args);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.exit(res);  
		}
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        if (args.length != 2) {
            throw new IOException("usage: [input] [output]");
        }
        
    	Configuration conf = getConf();
    	conf.set(TextOutputFormat.SEPERATOR, ",");
        Job job = Job.getInstance(conf);
        job.setJobName("Web log parsing");
        job.setJarByClass(getClass());
   
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Sequence file output with Snappy compression. Comment next 4 lines to use text output
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);        
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK);
        
//        job.setOutputFormatClass(TextOutputFormat.class);    //Uncomment this line to use text output or comment for sequence one
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatIntTuple.class); 
        
        job.setMapperClass(WebLogMapper.class);
        job.setCombinerClass(WebLogCombiner.class);
        job.setReducerClass(WebLogReducer.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
