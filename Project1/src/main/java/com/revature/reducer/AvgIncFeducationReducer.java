package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AvgIncFeducationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		/**
		 * The reducer receives a pre-calculated difference from the mapper. All it has to do is divide the difference
		 * by 16 (the time period over which we are calculating the average), and print it to the output
		 */
		
	double difference = 0.0;
		for (DoubleWritable value : values){
			difference = value.get();
		}
		
		double percentIncrease = (difference)/16.0;
		context.write(new Text(key), new DoubleWritable(percentIncrease));
	}	
}
