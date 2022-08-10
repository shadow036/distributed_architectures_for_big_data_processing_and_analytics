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
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String previous_failure = "";
    	int occurences = 0;
    	boolean flag = false;
    	
    	for(Text value: values) {
    		occurences += 1;
    		if(previous_failure != "" && previous_failure.equals(value.toString()) == false)
    			flag = true;
    		previous_failure = value.toString();
    	}
    	if(occurences >= 5 && flag == true)
    		context.write(key, new IntWritable(occurences));
    }
}
