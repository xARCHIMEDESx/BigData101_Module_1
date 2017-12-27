package com.epam.bigdata101.module1.homework1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LongestWordMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private int currentLongestWordLength = 1;
	private LongWritable length = new LongWritable();
	private Text longestWord = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		//Checking whether string is not empty. Splitting words and saving to arraylist
		if (line != null && !line.trim().isEmpty()) {			
			List<String> wordsInLine = new ArrayList<>(Arrays.asList(value.toString().split("[^a-zA-Z]+")));
			
			//Finding the length of longest word in the line
			int longestWordLength = wordsInLine
					.stream()
					.mapToInt(String::length)
					.max()
					.orElse(-1);
			
			/* In this section, first is checking whether the longest word in the current line is longer
			than the longest word in previous lines. If positive - getting all words with than length from the line
			and writing to the context. If negative - there is no need to push these words to the map phase,
			because there are already longer ones. */
			if (longestWordLength >= currentLongestWordLength) {				
				currentLongestWordLength = longestWordLength;				
				List<String> longestWordList = wordsInLine
						.stream()
						.filter(s -> s.length() == longestWordLength)
						.collect(Collectors.toList());
				
				for (String word : longestWordList) {
					length.set(longestWordLength);
					longestWord.set(word);
					context.write(length, longestWord);											 
				}
			}
		}
	}
}
