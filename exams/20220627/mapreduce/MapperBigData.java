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
    		String[] words = value.toString().split(",");
    		String patch_id = words[0];
    		String server_id = words[1];
    		String date = words[2];
    		int year = Integer.parseInt(date.substring(0, 4));
    		
            if(year == 2022)
            	context.write(new Text(server_id), new Text(patch_id + ',' + date));
    }
}
