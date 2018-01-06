package com.epam.bigdata101.module1.homework3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.epam.bigdata101.module1.homework3.util.StringIntTuple;

public class BiddingDatasetPartitioner extends Partitioner<StringIntTuple, IntWritable> {

	@Override
	public int getPartition(StringIntTuple key, IntWritable value, int numReduceTasks) {
		 // return int value, corresponding the number of reducer, which is set in mapper
		 return numReduceTasks == 0 ? 0 : key.getRight();
	}
}
