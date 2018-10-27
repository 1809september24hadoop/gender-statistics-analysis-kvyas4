package com.revature.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DeathRateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>  {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String code = "SP.DYN.CDRT.IN";
		String name = " Death Rate";
		String line = value.toString();
		line = line.substring(1,line.length()-2);
		String[] arr = line.split("\",\"");
		String emplCode = arr[3];
		String country = arr[0];
		if (arr[3].equals(code)){
			for ( int i = arr.length -1 ; i > 4 ; i--){
				if (arr[i].isEmpty()){
					continue;
				}
				else{
					double percentage = Double.parseDouble(arr[i]);
					context.write( new Text (country + name), new DoubleWritable(percentage));
				}
			}
		}
		
	}
}
