package com.revature.femalegrad;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemGradReducer extends Reducer <Text, IntWritable, Text, DoubleWritable> {
	
	public void reduce(Text values, Iterable<IntWritable> key, Context context)
			throws IOException, InterruptedException {
		String entry = values.toString();
		String[] entries = entry.split(",");
		/**
		 * above sets up data for the for loop to go
		 * below the for loot iters to calculate the values needed for output
		 */
		for(Integer iter = 1; iter < entries.length-5; iter++){
			if (entries[entries.length-iter] != "," ||entries[entries.length-iter] != null|entries[entries.length-iter] != ""){
				String value = entries[entries.length-iter];
				value = value.substring(1,value.length()-1);
				//check to make sure value isn't empty
				if (value.isEmpty()){
					continue;
				}
				String stack = entries[0]+","+entries[3];
				String[] keys = stack.split(",");
				//create key values to be used for output later
				if (keys[1].contains("primary")){
					context.write(new Text(keys[0].substring(1,keys[0].length()-1)+",HighSchool,"), new DoubleWritable(Double.parseDouble(value)));
				}
				else if(keys[1].contains("tertiary")){
					context.write(new Text(keys[0].substring(1,keys[0].length()-1)+",College,"), new DoubleWritable(Double.parseDouble(value)));
				}
				break;
			}
			else{
				continue;
			}
		}
		
	}
}