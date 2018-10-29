package com.revature.projectTest;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.mapper.DeathRateMapper;
import com.revature.reducer.DeathRateReducer;

public class MaxDeathRateTest {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapdriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducedriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	public static String test = "\"United States\",\"USA\",\"Death rate, crude (per 1,000 people)\",\"SP.DYN.CDRT.IN\",\"9.5\",\"9.3\",\"9.5\",\"9.6\",\"9.4\",\"9.4\",\"9.5\",\"9.4\",\"9.8\",\"9.5\",\"9.5\",\"9.3\",\"9.4\",\"9.3\",\"9.1\",\"8.8\",\"8.8\",\"8.6\",\"8.7\",\"8.5\",\"8.8\",\"8.6\",\"8.5\",\"8.6\",\"8.7\",\"8.7\",\"8.7\",\"8.6\",\"8.9\",\"8.8\",\"8.6\",\"8.6\",\"8.5\",\"8.8\",\"8.8\",\"8.8\",\"8.8\",\"8.7\",\"8.6\",\"8.6\",\"8.5\",\"8.5\",\"8.5\",\"8.4\",\"8.2\",\"8.3\",\"8.1\",\"8\",\"8.1\",\"7.9\",\"8\",\"8.1\",\"8.1\",\"8.2\",\"8.2\",\"8.2\",\"\",";
	
	@Before
	public void setUp(){
		//Setting up the mapper object. The mapper is then passed as a parameter to the mapdriver object on which the
		// subsequent mapper tests will run
		DeathRateMapper mapper = new DeathRateMapper();
		mapdriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapdriver.setMapper(mapper);
		
		//Setting up the reducer object which will be passed as a parameter to the reduce driver to run the tests
		DeathRateReducer reducer = new DeathRateReducer();
		reducedriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reducedriver.setReducer(reducer);
		
		//Setting up the mapreduce driver that will take the mapper object and the reducer object as inputs for testing
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		/**
		 * Map Driver will take as input the test string, and will output just the country and the difference between
		 * the latest and oldest recorded value between 2000 and 2016
		 */
		mapdriver.withInput(new LongWritable(1), new Text(test));
		mapdriver.withOutput(new Text("United States 2015:"), new DoubleWritable(8.2));
		mapdriver.runTest();
	}
	
	@Test
	public void testReducer(){
		/**
		 * Reduce Driver will take as input just the country as text and the percentage as a DoubleWritable and will
		 * output the very same.
		 */
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(8.2));
		reducedriver.withInput(new Text("United States 2015:"), values);
		reducedriver.withOutput(new Text("United States 2015:"), new DoubleWritable(8.2));
		reducedriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		/**
		 * MapReduceDriver will take as input the test string and output the final average increase
		 */
		mapReduceDriver.withInput(new LongWritable (1), new Text(test));
		mapReduceDriver.addOutput(new Text ("United States 2015:"), new DoubleWritable(8.2));
		mapReduceDriver.runTest();
	}


}
