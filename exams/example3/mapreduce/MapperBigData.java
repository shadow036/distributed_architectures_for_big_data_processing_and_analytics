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
    		int timeslot = Integer.parseInt(words[1].split(":")[0]);
    		String date = words[0];
    		String identifier = words[2];
    		float cpu_usage = Float.parseFloat(words[3]);
    		if(timeslot >= 9 && timeslot <= 17 && date.startsWith("2018/05") == true && cpu_usage >= 90.0)
    			context.write(new Text(identifier), new IntWritable(1));
            
    }
}
