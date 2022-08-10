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
            
            String company = words[4];
            String category = words[3];
            float price = Float.parseFloat(words[2]);
            int price_flag = 0;
            if(price == 0)
            	price_flag = 1;
            else
            	price_flag = 0;
            
            if(category.toLowerCase().equals("education"))
            	context.write(new Text(company), new IntWritable(price_flag));
            
    }
}
