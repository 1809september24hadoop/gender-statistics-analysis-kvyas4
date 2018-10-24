package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AvgIncFeducationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		
//		double previousValueholder = 0.0;
//		double previousValue = 0;
//		int counter = 0;
//		double sumofIncreases = 0.0;
//		double firstValue = 0.0;
//		for (DoubleWritable fv : values){
//			firstValue = fv.get();
//			break;		
//		}
//		for ( DoubleWritable value : values){
//			counter++;
//			previousValueholder = previousValue;
//			double increase = (value.get() - previousValueholder);
//			sumofIncreases += increase;
//			previousValue = value.get();
//		}
//		double finalsum = sumofIncreases - firstValue;
//		double finalAverage = finalsum/(counter -1);
		double difference = 0.0;
		for (DoubleWritable value : values){
			difference = value.get();
		}
		
		double percentIncrease = (difference)/16.0;
		context.write(new Text(key), new DoubleWritable(percentIncrease));
	}	
}
