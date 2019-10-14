package com.samuelsen.cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<WordKey, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(WordKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key.getWord(), key.getCount());

    }
}
