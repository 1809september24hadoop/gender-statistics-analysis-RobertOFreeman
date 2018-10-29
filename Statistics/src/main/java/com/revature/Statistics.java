package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.bountyhunt.BountyHuntMapper;
import com.revature.bountyhunt.BountyHuntReducer;
import com.revature.femalegrad.FemGradMapper;
import com.revature.femalegrad.FemGradReducer;
import com.revature.percentfememp.PercentFemEmpMapper;
import com.revature.percentfememp.PercentFemEmpReducer;
import com.revature.percentmaleemp.PercentMalEmpMapper;
import com.revature.percentmaleemp.PercentMalEmpReducer;
import com.revature.usfemeducation.FemEduReducer;
import com.revature.usfemeducation.femeduMapper;


public class Statistics {
	/**
	 * 
	 * @param args
	 * @throws Exception
	 * 
	 * 
	 * Main function that check the third argument of the system input
	 * for the number to compare to determine the map-reduce to do
	 * 
	 */
	public static void main(String[] args) throws Exception{
		if (args.length != 3){
			System.out.printf("Usage: Main <input dir> <output dir> <problem number>\n");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(Statistics.class);
		job.setJobName("Statistics Locator");
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		Integer inNum = Integer.parseInt(args[2]);
		if(inNum==0){
			job.setMapperClass(FemGradMapper.class);
			job.setReducerClass(FemGradReducer.class);
		}
		else if (inNum==1){
			job.setMapperClass(femeduMapper.class);
			job.setReducerClass(FemEduReducer.class);
		}
		else if (inNum==2){
			job.setMapperClass(PercentFemEmpMapper.class);
			job.setReducerClass(PercentFemEmpReducer.class);
		}
		else if (inNum==3){
			job.setMapperClass(PercentMalEmpMapper.class);
			job.setReducerClass(PercentMalEmpReducer.class);
		}
		else if (inNum==4){
			job.setMapperClass(BountyHuntMapper.class);
			job.setReducerClass(BountyHuntReducer.class);
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
