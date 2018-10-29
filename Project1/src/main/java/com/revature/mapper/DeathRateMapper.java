package com.revature.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DeathRateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>  {
	/**
	 * This mapper maps the countries for their death rate values per 1000. For every country it loops from the year 2016
	 * looking for the most recent value recorded and maps that for that country
	 */
	public static int IndexOfYearOfInterest = 44;
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String code = "SP.DYN.CDRT.IN"; //Code that recognizes the data field to look at
		String name = "";
		String line = value.toString();
		line = line.substring(1,line.length()-2);
		String[] arr = line.split("\",\"");
		String emplCode = arr[3];
		String country = arr[0];
		int year = 2016; // A year counter to keep a track of which year we are getting the information of
		if (arr[3].equals(code)){
			for ( int i = arr.length -1 ; i > IndexOfYearOfInterest ; i--){
				year--;
				if (arr[i].isEmpty()){
					continue;
				}
				else{
					double percentage = Double.parseDouble(arr[i]);
					context.write( new Text (name + country + " " + year + ":"), new DoubleWritable(percentage));
					break;
				}
			}
		}
		
	}
}
