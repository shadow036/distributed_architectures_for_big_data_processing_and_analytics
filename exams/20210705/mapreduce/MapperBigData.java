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
            int year = Integer.parseInt(words[0].substring(0, 4));
            String item_id = words[2];
            int purchased = 0;
            if(words[3].equals("true"))
            	purchased = 1;
            if(year == 2020)
            	context.write(new Text(item_id), new IntWritable(purchased));
            
    }
}
