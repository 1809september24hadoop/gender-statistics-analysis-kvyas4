package com.revature.drivers;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.mapper.MaleEmploymentMapper;
import com.revature.reducer.MaleEmploymentReducer;



public class MaleEmploymentIncrease {
public static void main(String[] args) throws Exception {
		
		if(args.length != 2){
			System.out.printf("Usage: Percent Change in male employment <input dir> <output dir> \n");
			System.exit(-1);
		}

		Job job = new Job();

		job.setJarByClass(FemalesLessThanThirtyPercent.class);
		job.setJobName("Percent Change in male employment from the year 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MaleEmploymentMapper.class);
		job.setReducerClass(MaleEmploymentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);


	}

}
