package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");
            
            String stock_id = words[0];
            int year = Integer.parseInt(words[1].split("/")[0]);
            String date = words[1];
            String price = words[3];
            String time = words[2];
            
            if(year == 2017 && stock_id.toLowerCase().equals("goog"))
            	context.write(NullWritable.get(), new Text(price + ',' + date + ',' + time));
            
            
            
    }
}
