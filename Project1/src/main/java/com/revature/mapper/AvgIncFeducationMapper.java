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
		String line = value.toString();
		line = line.substring(1,line.length()-2);
		String[] arr = line.split("\",\"");
		String country = arr[0];
		String countryCode = arr[1];
		String educationCode = arr[3];
		double percentageOne = 0.0;
		double percentageTwo = 0.0;
//If statements verify that the country is only USA, and that we are only looking at females for education records---------- 
		if (countryCode.equals("USA")){
			if(educationCode.contains("SE.") && educationCode.contains(".FE.") && educationCode.contains(".CUAT.")){
				
// Concatenating to the country string depending on the type of education received------------------------------------------				
				if (educationCode.contains(".BA.")) country += " (Bachelor's)";
				else if (educationCode.contains(".LO.")) country += " (Lower Secondary)";
				else if (educationCode.contains(".PRM.")) country += " (Primary Education)";
				else if (educationCode.contains(".PO.")) country += " (Post Secondary)";
				else if (educationCode.contains(".ST.")) country += " (Short-cycle Tertiary)";
				else if (educationCode.contains(".UP.")) country += " (Upper Secondary)";
				else if (educationCode.contains(".MS.")) country += " (Masters)";
				else country += " (Doctorate)";
//--------------------------------------------------------------------------------------------------------------------------
// Running two loops, one from the beginning index, and one from the last index to collect the latest values and prevent
// having to run the loop through the entire set of values.				
				for(int i = 44 ; i < arr.length ; i++){
					if (arr[i].isEmpty()){
						continue;
					}
					else {
						percentageOne = Double.parseDouble(arr[i]);
						break;
					}
				}
				for (int i = arr.length -1 ; i > 4 ; i--){
					if (arr[i].isEmpty()){
						continue;
					}
					else{
						percentageTwo = Double.parseDouble(arr[i]);
						break;
					}
				}
//Finding the difference between two extreme values and writing it to context-----------------------------------------------
				double difference = percentageTwo-percentageOne;
				context.write(new Text(country), new DoubleWritable(difference));
			}
		}
	}
}
