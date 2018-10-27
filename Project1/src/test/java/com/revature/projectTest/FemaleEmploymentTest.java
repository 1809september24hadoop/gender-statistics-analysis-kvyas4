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

import com.revature.mapper.FemaleEmploymentMapper;
import com.revature.reducer.FemaleEmploymentReducer;

public class FemaleEmploymentTest {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapdriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducedriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	public static String test = "\"World\",\"WLD\",\"Employment to population ratio, ages 15-24, female (%) (modeled ILO estimate)\",\"SL.EMP.1524.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.9209775210806\",\"44.3369166125827\",\"43.3866181987796\",\"42.7190027906233\",\"41.9284623430979\",\"40.9883005266392\",\"40.1017695754816\",\"39.0575878306524\",\"38.7254377658866\",\"38.2386328894543\",\"37.7593253306284\",\"37.3003331570291\",\"36.8452255152149\",\"36.630325378243\",\"36.342242494753\",\"35.9130333522635\",\"35.6825437629372\",\"35.0455029300714\",\"34.2350512299756\",\"33.4609818297412\",\"33.078381201859\",\"32.7876059608252\",\"32.4884762382602\",\"32.5274264961361\",\"32.47353741694\",\"32.3003268340032\",";
	
	
	/*
	 * Setting up the test
	 */
	@Before
	public void setUp(){
		//Setting up the mapper object. The mapper is then passed as a parameter to the mapdriver object on which the
		// subsequent mapper tests will run
		FemaleEmploymentMapper mapper = new FemaleEmploymentMapper();
		mapdriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapdriver.setMapper(mapper);
		
		//Setting up the reducer object which will be passed as a parameter to the reduce driver to run the tests
		FemaleEmploymentReducer reducer = new FemaleEmploymentReducer();
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
		mapdriver.withOutput(new Text("Female Employment increase"), new DoubleWritable(-5.9383060554511005));
		mapdriver.runTest();
	}
	
	@Test
	public void testReducer(){
		/**
		 * Reduce Driver will take as input just the country as text and the percentage as a DoubleWritable and will
		 * output the very same.
		 */
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(-5.9383060554511005));
		reducedriver.withInput(new Text("Female Employment increase"), values);
		reducedriver.withOutput(new Text("Female Employment increase"), new DoubleWritable(-0.3711441284656938));
		reducedriver.runTest();
	}
	
	@Test
	public void testMapReduce(){
		/**
		 * MapReduceDriver will take as input the test string and output the percent change	 
		 */
		mapReduceDriver.withInput(new LongWritable (1), new Text(test));
		mapReduceDriver.addOutput(new Text ("Female Employment increase"), new DoubleWritable(-0.3711441284656938));
		mapReduceDriver.runTest();
	}


}
