package com.revature.drivers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.mapper.EducationMapper;
import com.revature.reducer.PercentageFemaleReducer;


public class FemalesLessThanThirtyPercent {

		public static void main(String[] args) throws Exception{
			if(args.length != 2){
				System.out.printf("Usage: FemaleLessThanThirtyPercent <input dir> <output dir> \n");
				System.exit(-1);
			}

			Job job = new Job();

			job.setJarByClass(FemalesLessThanThirtyPercent.class);
			job.setJobName("Percentage of countries where female graduation is less than 30%");

			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapperClass(EducationMapper.class);

//			//Intermediate reducer called combiner
//			job.setCombinerClass(SumReducer.class);

			job.setReducerClass(PercentageFemaleReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			boolean success = job.waitForCompletion(true);
			System.exit(success ? 0 : 1);



		}
	}

