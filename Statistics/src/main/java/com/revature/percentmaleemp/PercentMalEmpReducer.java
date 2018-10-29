package com.revature.percentmaleemp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PercentMalEmpReducer extends Reducer <Text, IntWritable, Text, DoubleWritable> {
	Map <String,Double[]> world = new HashMap<String,Double[]>();
	Integer year = 2017;
	Integer stopyear = 2000;
	Double firstval = 0.0;
	Double difference = 0.0;
	Double pastval = 0.0;
	Integer pastyear = 0;
	Integer firstyear = 0;

	public void reduce(Text values, Iterable<IntWritable> key, Context context)
			throws IOException, InterruptedException {

		String entry = values.toString();
		String[] entries = entry.split(",");
		String stack = entries[0]+","+entries[3];
		String[] keys = stack.split(",");
		keys[0] = keys[0].substring(1, keys[0].length()-1);
		
		/**
		 * above creates a key list designed to pull data from the entries to be used later to help with output
		 * below is a for loop to check and find data
		 */
		
		
		for(Integer iter = 1; iter < entries.length-5; iter++){
			if (entries[entries.length-iter] != "," ||entries[entries.length-iter] != null|entries[entries.length-iter] != ""){
				String value = entries[entries.length-iter];
				value = value.substring(1,value.length()-1);
				// makes sure value isn't empty
				if (value.isEmpty()){
					continue;
				}
				// sets the first value
				if (firstval == 0.0){
					firstval = Double.parseDouble(value);
				}
				// sets the first year
				if (firstyear == 0){
					firstyear = year-iter;
				}
				// sets the past value, and past year
				if (pastval.equals(0.0)){
					pastval = Double.parseDouble(value);
					pastyear = year-iter;
				}
				// sets the past value, and past year
				else{
					String curyear = String.valueOf(year-iter);
					pastval = Double.parseDouble(value);
					pastyear = Integer.valueOf(curyear);
				}
				// set up the output for the world
				if(keys[0].contains("World")){
					Double[] valueskeep = {firstval,pastval};
					if(year-iter == stopyear){
						world.put("World", valueskeep);
						break;
					}
					else{
						continue;
					}
				}
				// breaks out of loop for the stopped year
				else{
					if(year-iter == stopyear){
						context.write(new Text(keys[0]+","), new DoubleWritable(firstval-pastval));
						break;
					}
				}
			}
			else{
				continue;
			}
		}
	}
	@Override
	// cleanup designed to output the world value
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			if(world.get("World")==null){
				
			}
			else{
			Double[] worldvals = world.get("World");
			context.write(new Text("Global,"), new DoubleWritable(worldvals[0]-worldvals[1]));
			}
		}
	}