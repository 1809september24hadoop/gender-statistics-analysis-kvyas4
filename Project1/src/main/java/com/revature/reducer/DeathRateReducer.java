package com.revature.reducer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.util.concurrent.AtomicDouble;

public class DeathRateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	/**
	 * This reducer calculates the maximum of all the values it goes through and outputs the country with the
	 * highest death rate in their latest recorded measures
	 */
	
	public static volatile String CURRENT_MAX_WORD = null;
	public static AtomicDouble CURRENT_MAX_COUNT = new AtomicDouble(Integer.MIN_VALUE);
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		
		for (DoubleWritable value :values){
			CURRENT_MAX_COUNT = (value.get() > CURRENT_MAX_COUNT.get()) ? new AtomicDouble(value.get()) : CURRENT_MAX_COUNT;
			CURRENT_MAX_WORD = (value.get()== CURRENT_MAX_COUNT.get()) ? key.toString() : CURRENT_MAX_WORD;
		}
				
	}
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		context.write(new Text(CURRENT_MAX_WORD), new DoubleWritable(CURRENT_MAX_COUNT.get()));
	}
}
