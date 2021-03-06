package com.revature.percentfememp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PercentFemEmpMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	/**
	 * sieves the data filtering for the female employment ratio
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		Integer linenum = 0;
		String line = value.toString();
		String[] lines = line.split(",");
		if(lines[2].contains("Employment to population ratio")){
			if (lines[3].contains("15+")){
				if(lines[4].contains("female")){
					if(lines[4].contains("ILO")){
						context.write(new Text(line), new IntWritable(linenum++));
					}
				}
			}
		}
	}
}
