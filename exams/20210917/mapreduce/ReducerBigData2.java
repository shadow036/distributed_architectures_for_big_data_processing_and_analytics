package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int max_purchases = -1;
    	String max_user = null;
    	
    	for(Text value: values) {
    		String[] elements = value.toString().split("_");
    		Integer purchases = Integer.parseInt(elements[1]);
    		String username = elements[0];
    		if(purchases > max_purchases || (purchases == max_purchases && (username.toLowerCase().compareTo(max_user) < 0 || max_user == null))) {
    			max_purchases = purchases;
    			max_user = username;
    		}
    	}
    	
    	context.write(new Text(max_user), new IntWritable(max_purchases));
    }
}
