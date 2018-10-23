package com.revature.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AvgIncFeducationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String code1 = "SE.TER.CUAT.BA.FE.ZS";
		String code2 = "SE.TER.CUAT.MS.FE.ZS";
		String line = value.toString();
		line = line.substring(1,line.length()-2);
		String[] arr = line.split("\",\"");
		String country = arr[0];
		String countryCode = arr[1];
		String educationCode = arr[3];
		if (countryCode.equals("USA")){
			if(educationCode.equals(code1)|| educationCode.equals(code2)){
				for(int i =4 ; i < arr.length ; i++){
					if (arr[i].isEmpty()){
						continue;
					}
					double percentage = Double.parseDouble(arr[i]);
					context.write(new Text(country), new DoubleWritable(percentage));

				}
			}
		}
	}
}
