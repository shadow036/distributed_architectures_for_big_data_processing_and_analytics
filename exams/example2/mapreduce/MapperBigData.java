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
                    IntWritable> {// Output value type
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		String[] words = value.toString().split(",");
    		String city = words[1];
    		float max = Float.parseFloat(words[3]);
    		float min = Float.parseFloat(words[4]);
    		int code = 0;
    		if(max > 35)
    			code += 2;
    		if(min < -20)
    			code += 1;
    		/*
    		0: max <= 35 and min >= -20
    		1: max <= 35 and min < -20
    		2: max > 35 and min >= -20
    		3: max > 35 and min < -20
    		*/
    		if(code > 0)
    			context.write(new Text(city), new IntWritable(code));
            
    }
}
