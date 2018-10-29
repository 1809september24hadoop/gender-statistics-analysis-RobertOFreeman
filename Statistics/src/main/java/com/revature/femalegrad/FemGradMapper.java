package com.revature.femalegrad;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemGradMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	// filters out the graduation data for females
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		Integer linenum = 0;
		String line = value.toString();
		String[] lines = line.split(",");
		if(lines[2].contains("graduation")){
			if(lines[4].contains("female")){
			context.write(new Text(line), new IntWritable(linenum++));
			}
		}
	}
}