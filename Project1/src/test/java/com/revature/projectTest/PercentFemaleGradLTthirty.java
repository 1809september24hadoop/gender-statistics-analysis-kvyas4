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

import com.revature.mapper.EducationMapper;
import com.revature.reducer.PercentageFemaleReducer;

public class PercentFemaleGradLTthirty {
/**
 * Declaring harness to test mapper, reducer and mapreduce
 */
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapdriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducedriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	public static String goodtest = "\"Colombia\",\"COL\",\"Educational attainment, at least completed post-secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.PO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.81603\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.91467\",\"19.67238\",\"20.91326\",\"20.74789\",\"\",";
	public static String badtest = "\"Israel\",\"ISR\",\"Borrowed to start, operate, or expand a farm or business, female (% age 15+) [w2]\",\"WP14923.3\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"3.905059\",\"\",\"\",";
	
	
	/*
	 * Setting up the test
	 */
	@Before
	public void setUp(){
		//Setting up the mapper object. The mapper is then passed as a parameter to the mapdriver object on which the
		// subsequent mapper tests will run
		EducationMapper mapper = new EducationMapper();
		mapdriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapdriver.setMapper(mapper);
		
		//Setting up the reducer object which will be passed as a parameter to the reduce driver to run the tests
		PercentageFemaleReducer reducer = new PercentageFemaleReducer();
		reducedriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reducedriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper(){
		/**
		 * Map Driver will take as input the good test string, and will output just the country and latest value of
		 * female percentage found that is less than 30%
		 */
		mapdriver.withInput(new LongWritable(1), new Text (goodtest));
		mapdriver.withOutput(new Text("Colombia"), new DoubleWritable(20.74789));
		mapdriver.runTest();
	}
	
	@Test
	public void testReducer(){
		/**
		 * Reduce Driver will take as input just the country as text and the percentage as a DoubleWritable and will
		 * output the very same.
		 */
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(20.74789));
		reducedriver.withInput(new Text("Colombia"), values);
		reducedriver.withOutput(new Text("Colombia"), new DoubleWritable(20.74789));
		reducedriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		mapReduceDriver.withInput(new LongWritable (1), new Text(goodtest));
		mapReduceDriver.addOutput(new Text ("Colombia"), new DoubleWritable(20.74789));
		mapReduceDriver.runTest();
	}
	

}
