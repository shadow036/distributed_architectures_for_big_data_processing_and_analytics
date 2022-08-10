package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
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
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int counter = 0;
    	int counter_true = 0;
    	
    	for(IntWritable value: values) {
    		counter += 1;
    		counter_true += value.get();
    	}
    	float conversion_rate = 100 * counter_true/counter;
    	if(conversion_rate > 20)
    		context.write(key, new FloatWritable(conversion_rate));
    }
}
