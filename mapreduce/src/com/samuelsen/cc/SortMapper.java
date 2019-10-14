package com.samuelsen.cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, WordKey, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\\s+");

        Text word = new Text(values[0]);
        IntWritable count = new IntWritable(Integer.parseInt(values[1]));

        context.write(new WordKey(word, count), count);
    }
}
