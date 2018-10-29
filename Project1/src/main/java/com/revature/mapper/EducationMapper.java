package com.revature.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EducationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	/**
	 * The mapper looks at the data fields that have the Post secondary education code associated with them. It then
	 * goes through all the values from the latest recorded value in a reverse order and when it finds it's first record,
	 * it checks to see if it is less then 30%. If true, the value, along with the country name as the key is mapped for
	 * the reducer.
	 */
		public static int StartOfNumericValues = 4;
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			String line = value.toString();
			line = line.substring(1, line.length()-2);
			String[] arr = line.split("\",\"");
			String country = arr[0];
			String code = arr[3];
			if (code.contains(".FE.") && code.contains("SE.") && code.contains(".CUAT.")){
				if(code.substring(12,14).equals("PO")) {
					for (int i = arr.length-1; i > StartOfNumericValues ; i--){
						if (arr[i].isEmpty()){
							continue;
						}
						else {
							double percentage = Double.parseDouble(arr[i]);
							if (percentage < 30.0){
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
