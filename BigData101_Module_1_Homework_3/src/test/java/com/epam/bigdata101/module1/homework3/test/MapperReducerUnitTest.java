package com.epam.bigdata101.module1.homework3.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.epam.bigdata101.module1.homework3.BiddingDatasetMapper;
import com.epam.bigdata101.module1.homework3.BiddingDatasetReducer;
import com.epam.bigdata101.module1.homework3.util.StringIntTuple;

public class MapperReducerUnitTest {
	MapDriver<LongWritable, Text, StringIntTuple, IntWritable> mapDriver;
	ReduceDriver<StringIntTuple, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, StringIntTuple, IntWritable, Text, IntWritable> mapReduceDriver;
	 
	@Before
	public void setUp() {
		BiddingDatasetMapper mapper = new BiddingDatasetMapper();
	    BiddingDatasetReducer reducer = new BiddingDatasetReducer();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test 
	public void testMapper() throws IOException {
		mapDriver.withCacheFile("city.en.txt");
		mapDriver.withCacheFile("region.en.txt");;
		mapDriver.withInput(new LongWritable(), new Text("da57aa9205e1c57e5c99a6f657cb6b0d	20131019002000352"
				+ "	1	D1TMbZ0Oefa	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)	221.4.142.*"
				+ "	216	233	2	adf2a07e81fd1c55205de67c326424f8	null	null	2754779263	728	90	OtherView"
				+ "	Na	5	7330	277	43	null	2259	16593,14273,10075,10006,13866,10110,10126,11944,13403,10063,10116"));
		mapDriver.withOutput(new StringIntTuple("dongguan", 0), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(2));
		values.add(new IntWritable(3));
		reduceDriver.withInput(new StringIntTuple("beijing", 1), values);
		reduceDriver.withOutput(new Text("beijing"), new IntWritable(6));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapperReducer() throws IOException{
		mapReduceDriver.withCacheFile("city.en.txt");
		mapReduceDriver.withCacheFile("region.en.txt");;
		
		mapReduceDriver.withInput(new LongWritable(), new Text("2ce53bf4770bc0c7e3dafce544f77071	20131021213303860	1	D51GUo5IJv0"
				+ "	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0"
				+ "	114.232.232.*	80	86	3	3043163ba84753b4b51dd3290caeae67	bfa000ed663f4997db218d6da6b1510f	null"
				+ "	News_Width4	1000	90	Na	Na	50	10734	294	68	null	2821	10006"));
		mapReduceDriver.withInput(new LongWritable(), new Text("c5a28e8df929a2c5710660f50bd75d9f	20131021233007581"
				+ "	1	CB5Mdu5EdGR	Mozilla/5.0 (Windows NT 6.3; ARM; Trident/7.0; Touch; rv:11.0) like Gecko	183.239.60.*"
				+ "	216	216	4	ba4799f049b70d09c98b00a5eca31620	2ac15f38e07322f73883efb26553d9f2	null"
				+ "	9223372032560363195	960	90	FirstView	Na	0	10717	294	1	null	2821	14273,13042,10006,10110,10063"));
		mapReduceDriver.withInput(new LongWritable(), new Text("bd1cc3070f959822cf9b81913120e8d4	20131021213803668"
				+ "	1	CCF19YEWbjB	Opera/9.80 (Windows NT 5.1) Presto/2.12.388 Version/12.14	114.231.110.*	80	86	4"
				+ "	b6ecc007215e65c632afc2bc9c93f003	fed8bb10c48a75117ab871ed9004b652	null	9223372032559872581	300	250"
				+ "	SecondView	Na	30	10722	294	31	null	2821	10057,13800,13042,10110,10031,10133,10063,10116"));
		
		mapReduceDriver.withOutput(new Text("guangdong"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("nantong"), new IntWritable(2));
		mapReduceDriver.runTest();
	}
	
}
