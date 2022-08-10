package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
	
	    	String[] w = value.toString().split(",");
	    	String country = w[4];
	    	String datacenter_id = w[2];
	    	String cpu = w[1];
	    	
	    	if(country.toLowerCase().equals("spain"))
	    		context.write(new Text(datacenter_id), new Text(cpu));
            
    }
}
