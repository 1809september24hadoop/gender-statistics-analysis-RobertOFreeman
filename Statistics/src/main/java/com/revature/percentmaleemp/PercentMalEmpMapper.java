package com.revature.percentmaleemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PercentMalEmpMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	// map design to sieve out the males population data  that are employed and above 15 years old
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		Integer linenum = 0;
		String line = value.toString();
		String[] lines = line.split(",");
		if(lines[2].contains("Employment to population ratio")){
			if (lines[3].contains("+")){
				if(lines[4].contains("ILO")){
					if(!(lines[4].contains("female"))){
						context.write(new Text(line), new IntWritable(linenum++));

					}
				}
			}
		}
	}
}