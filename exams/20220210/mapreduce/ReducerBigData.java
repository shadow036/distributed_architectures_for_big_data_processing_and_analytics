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
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int free_apps_count = 0;
    	int all_apps_count = 0;
    	
    	for(IntWritable app: values) {
    		free_apps_count += app.get();
    		all_apps_count += 1;
    	}
    	
    	if(free_apps_count == all_apps_count)
    		context.write(key, new IntWritable(free_apps_count));
    }
}
