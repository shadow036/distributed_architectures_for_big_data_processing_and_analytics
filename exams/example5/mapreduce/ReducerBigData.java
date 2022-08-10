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

    	String cpu = null;
    	boolean flag = true;
    	
    	for(Text v: values) {
    		if(cpu != null && !v.toString().equals(cpu))
    			flag = false;
    		cpu = v.toString();
    	}
    	if(flag == true)
    		context.write(key, new Text(cpu));
    }
}
