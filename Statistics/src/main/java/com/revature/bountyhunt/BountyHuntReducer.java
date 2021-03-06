package com.revature.bountyhunt;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BountyHuntReducer extends Reducer <Text, IntWritable, Text, DoubleWritable> {
	Double femaleval = 0.0;
	// creates a global value to be used later
	// for loop iterates through values of lines to check for biases
	public void reduce(Text values, Iterable<IntWritable> key, Context context)
			throws IOException, InterruptedException {
		String entry = values.toString();
		String[] entries = entry.split(",");
		String country = entries[0];
		country = country.substring(1,country.length()-1);
		// gets the countries name
		for(Integer iter = 1; iter < entries.length-5; iter++){
			if (entries[entries.length-iter] != "," ||entries[entries.length-iter] != null|entries[entries.length-iter] != ""){
				String value = entries[entries.length-iter];
				value = value.substring(1,value.length()-1);
				// makes sure value isnt empty
				if (value.isEmpty()){
					continue;
				}
				// checks and changes values based off the gender of the row
				if(entries[3].contains("female")){
					femaleval = Double.parseDouble(value);
					break;
				}
				// check and does calculations if the gender of the row is male
				else if (entries[3].contains("male")){
					Double maleval = Double.parseDouble(value);
					// checks to see if the female timing is larger
					if(femaleval > maleval){
						context.write(new Text(country +","+"female bias,difference,"), new DoubleWritable(femaleval-maleval));
						femaleval = 0.0;
						maleval = 0.0;
						break;
					}
					//check to see if the male timing is larger
					else if (maleval > femaleval){
						context.write(new Text(country +","+"male bias,difference,"), new DoubleWritable(maleval-femaleval));
						femaleval = 0.0;
						maleval = 0.0;
						break;
					}
					// what is performed if there is no bias
					else{
						femaleval = 0.0;
						maleval = 0.0;
						break;
					}
				}
			}
			else{
				continue;
			}
		}
	}
}