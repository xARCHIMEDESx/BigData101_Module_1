package com.epam.bigdata101.module1.homework2.test;

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

import com.epam.bigdata101.module1.homework2.WebLogMapper;
import com.epam.bigdata101.module1.homework2.WebLogReducer;
import com.epam.bigdata101.module1.homework2.util.FloatIntTuple;

public class MapperReducerUnitTest {
	MapDriver<LongWritable, Text, Text, FloatIntTuple> mapDriver;
	ReduceDriver<Text, FloatIntTuple, Text, FloatIntTuple> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, FloatIntTuple, Text, FloatIntTuple> mapReduceDriver;
	 
	@Before
	public void setUp() {
		WebLogMapper mapper = new WebLogMapper();
	    WebLogReducer reducer = new WebLogReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] "
				+ "\"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" "
				+ "\"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
		mapDriver.withOutput(new Text("ip1"), new FloatIntTuple(1, 40028));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<FloatIntTuple> values = new ArrayList<>();
		values.add(new FloatIntTuple(5.0f, 2000));
		values.add(new FloatIntTuple(3.0f, 3000));
		values.add(new FloatIntTuple(2.0f, 5000));
		reduceDriver.withInput(new Text("ip15"), values);
		reduceDriver.withOutput(new Text("ip15"), new FloatIntTuple(1000.0f, 10000));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException{
		mapReduceDriver.withInput(new LongWritable(), new Text("ip7 - - [24/Apr/2011:04:31:21 -0400] "
				+ "\"GET /sgi_indy/indy_monitor.jpg HTTP/1.1\" 200 12805 "
				+ "\"http://machinecity-hello.blogspot.com/2008_01_01_archive.html\" "
				+ "\"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10\""));
		mapReduceDriver.withInput(new LongWritable(), new Text("ip7 - - [24/Apr/2011:04:31:22 -0400] "
				+ "\"GET /sgi_indy/indy_inside.jpg HTTP/1.1\" 200 23285 "
				+ "\"http://machinecity-hello.blogspot.com/2008_01_01_archive.html\" "
				+ "\"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10\""));
		mapReduceDriver.withInput(new LongWritable(), new Text("ip56 - - [24/Apr/2011:07:54:05 -0400] "
				+ "\"GET /robots.txt HTTP/1.0\" 404 274 \"-\" \"Mozilla/5.0 ()\""));
		mapReduceDriver.withOutput(new Text("ip56"), new FloatIntTuple(274f, 274));
		mapReduceDriver.withOutput(new Text("ip7"), new FloatIntTuple(18045.0f, 36090));		
		mapReduceDriver.runTest();
	}
	
}
