package com.revature.drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.mapper.DeathRateMapper;
import com.revature.reducer.AvgIncFeducationReducer;
import com.revature.reducer.DeathRateReducer;

public class MaxDeathRate {

	public static void main(String[] args) throws Exception{

		if(args.length != 2){
			System.out.printf("Usage: Highest Death Rate <input dir> <output dir> \n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(FemalesLessThanThirtyPercent.class);
		job.setJobName("Country with the highest death rate");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(DeathRateMapper.class);
		job.setReducerClass(DeathRateReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
