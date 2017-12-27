package com.epam.bigdata101.module1.homework1.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.epam.bigdata101.module1.homework1.LongestWordMapper;
import com.epam.bigdata101.module1.homework1.LongestWordReducer;

public class MapperReducerUnitTest {
	private MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	private ReduceDriver<LongWritable, Text, LongWritable, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

	@Before
	public void setUp() {
		LongestWordMapper mapper = new LongestWordMapper();
		LongestWordReducer reducer = new LongestWordReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("Hello!!!"));
		mapDriver.withOutput(new LongWritable(5), new Text("Hello"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> values = new ArrayList<>();
		values.add(new Text("world"));
		values.add(new Text("earth"));
		reduceDriver.withInput(new LongWritable(5), values);
		reduceDriver.withOutput(new LongWritable(5), new Text(values.get(0) + " " + values.get(1) + " "));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException{
		mapReduceDriver.withInput(new LongWritable(), new Text("My name is Vitaliy Pivovar"));
		mapReduceDriver.withInput(new LongWritable(), new Text("Welcome to Lviv"));
		mapReduceDriver.withOutput(new LongWritable(7), new Text("Vitaliy " + "Pivovar " + "Welcome "));
		mapReduceDriver.runTest();
	}
}