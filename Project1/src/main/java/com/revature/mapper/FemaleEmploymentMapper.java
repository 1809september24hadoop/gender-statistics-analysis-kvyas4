package com.revature.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FemaleEmploymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String ILOCode = "SL.EMP.1524.SP.FE.ZS";
		String name = "Female Employment increase";
		String line = value.toString();
		line = line.substring(1,line.length()-2);
		String[] arr = line.split("\",\"");
		String emplCode = arr[3];
		String code = arr[1];
		double percentageTwo = 0.0;
		double percentageOne = 0.0;
		if (arr[1].equals("WLD")){
			if(emplCode.equals(ILOCode)){
				for (int i = 44 ; i < arr.length ; i++){
					if(arr[i].isEmpty()){
						continue;			
						}
					else{
						percentageOne = Double.parseDouble(arr[i]);
						break;
					}
				}
				for (int i = arr.length -1 ; i > 44 ; i--){
					if(arr[i].isEmpty()){
						continue;
					}
					else{
						percentageTwo = Double.parseDouble(arr[i]);
						break;
					}
				}
				double change = (percentageTwo-percentageOne);
				context.write(new Text (name), new DoubleWritable(change));
			}
		}
	}


}
