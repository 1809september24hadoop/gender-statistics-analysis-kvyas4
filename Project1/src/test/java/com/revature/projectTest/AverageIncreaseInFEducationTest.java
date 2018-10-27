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

import com.revature.mapper.AvgIncFeducationMapper;
import com.revature.reducer.AvgIncFeducationReducer;



public class AverageIncreaseInFEducationTest {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapdriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducedriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	public static String test = "\"United States\",\"USA\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"31.39076\",\"32.00147\",\"32.67396\",\"\",";
	
	
	/*
	 * Setting up the test
	 */
	@Before
	public void setUp(){
		//Setting up the mapper object. The mapper is then passed as a parameter to the mapdriver object on which the
		// subsequent mapper tests will run
		AvgIncFeducationMapper mapper = new AvgIncFeducationMapper();
		mapdriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapdriver.setMapper(mapper);
		
		//Setting up the reducer object which will be passed as a parameter to the reduce driver to run the tests
		AvgIncFeducationReducer reducer = new AvgIncFeducationReducer();
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
		mapdriver.withOutput(new Text("United States (Bachelor's)"), new DoubleWritable(1.2832000000000008));
		mapdriver.runTest();
	}
	
	@Test
	public void testReducer(){
		/**
		 * Reduce Driver will take as input just the country as text and the percentage as a DoubleWritable and will
		 * output the very same.
		 */
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1.2832000000000008));
		reducedriver.withInput(new Text("United States (Bachelor's)"), values);
		reducedriver.withOutput(new Text("United States (Bachelor's)"), new DoubleWritable(0.08020000000000005));
		reducedriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		/**
		 * MapReduceDriver will take as input the test string and output the final average increase
		 */
		mapReduceDriver.withInput(new LongWritable (1), new Text(test));
		mapReduceDriver.addOutput(new Text ("United States (Bachelor's)"), new DoubleWritable(0.08020000000000005));
		mapReduceDriver.runTest();
	}


}
