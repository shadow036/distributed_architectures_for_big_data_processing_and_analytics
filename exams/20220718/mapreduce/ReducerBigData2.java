package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
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

    	String first_date = null;
    	int max_robots = 0;
    	
    	for(Text pair: values) {
    		String[] elements = pair.toString().split(",");
    		String date = elements[0];
    		int number_of_robots = Integer.parseInt(elements[1]);
    		if(first_date == null || number_of_robots > max_robots || (number_of_robots == max_robots && date.compareTo(first_date) < 0)) {
    			first_date = date;
    			max_robots = number_of_robots;
    		}
    	}

    	context.write(new Text(first_date), new IntWritable(max_robots));
    }
}
