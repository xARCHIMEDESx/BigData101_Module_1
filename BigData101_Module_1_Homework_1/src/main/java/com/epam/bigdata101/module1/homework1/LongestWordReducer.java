package com.epam.bigdata101.module1.homework1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LongestWordReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	private boolean longestWordDone = false;
	private Text longestWord = new Text();

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		/* As the data, that coming to the combiner/reducer is sorted in decreasing key order with the longest words at the top,
		there is no reason to process records except the first one. */
		if (!longestWordDone) {
			String words = "";
			for (Text value : values) {
				words += (value.toString() + " ");
			}
			longestWord.set(words);
			context.write(key, longestWord);
			
			longestWordDone = true;
		}
	}
}
