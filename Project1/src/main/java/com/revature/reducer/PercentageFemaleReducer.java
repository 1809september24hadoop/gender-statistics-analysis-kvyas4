package com.revature.reducer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PercentageFemaleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException{
		
		
		for (DoubleWritable value : values){
			context.write(key, value);
		}
	}

}
