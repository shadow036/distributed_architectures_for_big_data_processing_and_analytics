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
            String city = words[3];
            String country = words[4];
            String category = words[5];
            String subcategory = words[6];
            Integer subcategory_flag = 0;
            if(country.toLowerCase().equals("italy") && category.toLowerCase().equals("tourism")) {
            	if(subcategory.toLowerCase().equals("museum"))
	           		subcategory_flag = 1;
	           	context.write(new Text(city), new IntWritable(subcategory_flag));
            }
    	}
    }
