package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String last_patch = "";
    	String last_date = "";
    	for(Text value: values) {
    		String[] elements = value.toString().split(",");
    		if(last_patch.equals("") || elements[1].compareTo(last_date) > 0) {
    			last_patch = elements[0];
    			last_date = elements[1];
    		}
    	}
    	context.write(key, new Text(last_patch));
    }
}
