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

    	int purchases_in_2010 = 0;
    	int purchases_total = 0;
    	
    	for(IntWritable value: values) {
    		purchases_in_2010 += value.get();
    		purchases_total += 1;
    	}
    	if(purchases_in_2010 == purchases_total)
    		context.write(key, new IntWritable(purchases_in_2010));
    }
}
