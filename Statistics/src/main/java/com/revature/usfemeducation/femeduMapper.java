package com.revature.usfemeducation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class femeduMapper extends Mapper<LongWritable, Text, Text, Text> {

		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			
			String line = value.toString();
			
			if(line.contains("employment")&& line.contains("male")){
				String[] lines = line.split(",");
			context.write(new Text(lines[0]), new Text(line));
			}
			
			
			
		}
}
