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

import com.revature.mapper.MaleEmploymentMapper;
import com.revature.reducer.MaleEmploymentReducer;



public class MaleEmploymentTest {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapdriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducedriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	public static String test = "\"World\",\"WLD\",\"Employment to population ratio, ages 15-24, male (%) (modeled ILO estimate)\",\"SL.EMP.1524.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"59.4520017301749\",\"59.1130336706299\",\"58.3489131004968\",\"57.6636990167024\",\"57.256937191903\",\"56.5189007081833\",\"55.8100981469673\",\"54.5962138193301\",\"54.7192226260086\",\"54.0860405647094\",\"53.5953144181414\",\"52.6358543871313\",\"52.1200811050426\",\"52.0853433686755\",\"51.9013491045367\",\"51.4353348678587\",\"51.1670232363033\",\"50.4490972886737\",\"49.3227483516913\",\"48.6235421444207\",\"48.1320298875564\",\"47.7323595600081\",\"47.3311659501936\",\"47.0985071044499\",\"47.1629933752477\",\"47.1523810181231\",";
	
	
	/*
	 * Setting up the test
	 */
	@Before
	public void setUp(){
		//Setting up the mapper object. The mapper is then passed as a parameter to the mapdriver object on which the
		// subsequent mapper tests will run
		MaleEmploymentMapper mapper = new MaleEmploymentMapper();
		mapdriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapdriver.setMapper(mapper);
		
		//Setting up the reducer object which will be passed as a parameter to the reduce driver to run the tests
		MaleEmploymentReducer reducer = new MaleEmploymentReducer();
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
		 * Map Driver will take as input the test string, and will output the difference between
		 * the latest and oldest recorded value between of male employment 2000 and 2016
		 */
		mapdriver.withInput(new LongWritable(1), new Text(test));
		mapdriver.withOutput(new Text("Male Employment increase"), new DoubleWritable(-6.9336595465862985));
		mapdriver.runTest();
	}
	
	@Test
	public void testReducer(){
		/**
		 * Reduce Driver will take as input just the country as text and the percentage as a DoubleWritable and will
		 * output the very same.
		 */
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(-6.9336595465862985));
		reducedriver.withInput(new Text("Male Employment increase"), values);
		reducedriver.withOutput(new Text("Male Employment increase"), new DoubleWritable(-0.43335372166164365));
		reducedriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		/**
		 * MapReduceDriver will take as input the test string and output the percent change	 
		 */
		mapReduceDriver.withInput(new LongWritable (1), new Text(test));
		mapReduceDriver.addOutput(new Text ("Male Employment increase"), new DoubleWritable(-0.43335372166164365));
		mapReduceDriver.runTest();
	}

}
