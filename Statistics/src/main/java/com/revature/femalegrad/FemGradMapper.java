package com.revature.femalegrad;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemGradMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{

		String line = value.toString();

		if(line.contains("female")&&line.contains("graduation")){
			String[] lines = line.split(",");
		context.write(new Text(lines[0]), new Text(line));
		}
	}
}