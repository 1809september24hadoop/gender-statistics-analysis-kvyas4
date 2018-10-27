package com.revature.reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaleEmploymentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{

		double percentIncrease = 0.0;
		for (DoubleWritable value : values){
			percentIncrease = value.get();
		}
		double change = percentIncrease/16.0;
		context.write(key, new DoubleWritable(change));
	}
}
