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
                NullWritable,           // Input key type
                IntWritable,    // Input value type
                IntWritable,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int younger = (int) (Double.NEGATIVE_INFINITY);
        int counter = 0;
        for(IntWritable year: values) {
        	if(year.get() > younger ) {
        		younger = year.get();
        		counter = 1;
        	}else if(year.get() == younger) {
        		counter += 1;
        	}
        }
        context.write(new IntWritable(younger), new IntWritable(counter));
    }
}
