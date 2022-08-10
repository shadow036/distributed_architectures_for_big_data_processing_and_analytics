package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                FloatWritable,           // Output key type
                Text> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	float max = 36;
    	String first_date = "";
    	
    	for(Text e: values) {
    		String[] w = e.toString().split(",");
    		float price = Float.parseFloat(w[0]);
    		String datetime = w[1] + ',' + w[2];
    		if(price > max || first_date == "" || (max == price && datetime.compareTo(first_date) < 0)) {
    			max = price;
    			first_date = datetime;
    		}
    	}
    	context.write(new FloatWritable(max), new Text(first_date));
    	
    }
}
