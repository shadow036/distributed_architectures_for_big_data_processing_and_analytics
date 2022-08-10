package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
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
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int category_counter = 0;
    	int subcategory_counter = 0;
    	
    	for(IntWritable flag: values) {
    		category_counter += 1;
    		subcategory_counter += flag.get();
    	}
    	if(category_counter >= 2 && subcategory_counter >= 1) {
    		context.write(key, NullWritable.get());
    	}
    }
}
