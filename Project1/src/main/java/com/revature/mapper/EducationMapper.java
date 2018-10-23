package com.revature.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EducationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			String line = value.toString();
			line = line.substring(1, line.length()-2);
			String[] arr = line.split("\",\"");
			String country = arr[0];
			String code = arr[3];
			if (code.contains(".FE.") && code.contains("SE.") && code.contains(".CUAT.")){
				if(code.substring(12,14).equals("UP")) {
					int currentYear = 2017;
					for (int i = arr.length-1; i > 4 ; i--){
						currentYear--;
						if (arr[i].isEmpty()){
							continue;
						}
						else {
							double percentage = Double.parseDouble(arr[i]);
							if (percentage < 30.0){
								country = country + " (" + currentYear + ") :";
								context.write( new Text(country), new DoubleWritable(percentage));
								break;
							}
							else break;
						}
					}
				}
			}
		}
}
