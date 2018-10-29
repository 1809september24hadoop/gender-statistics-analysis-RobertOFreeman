package com.revature.usfemeducation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemEduReducer extends Reducer <Text, IntWritable, Text, DoubleWritable> {

	Map<String,Double> avgtert = new HashMap<String,Double>();
	Integer year = 2017;
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
		/**
		 * above gets the values we need
		 * then separates the values we will need later
		 */
		
		
		
		/**
		 * below we create a for loop in order to actually calculate the value of difference of graduates
		 */
		for(Integer iter = 1; iter < entries.length-5; iter++){
			if (entries[entries.length-iter] != "," ||entries[entries.length-iter] != null|entries[entries.length-iter] != ""){
				String value = entries[entries.length-iter];
				value = value.substring(1,value.length()-1);
				// makes sure the value isnt empty
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
				// sets the past value and past year
				if(keys[1].contains("tertiary")){
					if (pastval.equals(0.0)){
						pastval = Double.parseDouble(value);
						pastyear = year-iter;
					}
					else{
						String curyear = String.valueOf(year-iter);
						difference = (pastval-Double.parseDouble(value));
						avgtert.put(new String(curyear+"-"+pastyear), difference);
						pastval = Double.parseDouble(value);
						pastyear = Integer.valueOf(curyear);
					}
				}
				// breaks the loop if the year hits the last year required
				if (year-iter == 2000){
					break;
				}
			}
			else{
				continue;
			}
		}
	}
	@Override
	// so the output is created evenly we overide cleanup with a staged output
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		context.write(new Text("Global Increase,"), new DoubleWritable((firstval-pastval)/(firstyear-pastyear)));
		for (String avgyears : avgtert.keySet()){
			context.write(new Text(avgyears+","), new DoubleWritable(avgtert.get(avgyears)));
		}
	}
}