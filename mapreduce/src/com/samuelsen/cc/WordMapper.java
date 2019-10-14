package com.samuelsen.cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final Map<String, Integer> wordMap = new HashMap<>();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String token = "";
        while (tokenizer.hasMoreTokens()) {
            token = tokenizer.nextToken();
            if (wordMap.containsKey(token)) wordMap.put(token,wordMap.get(token) + 1);
            else wordMap.put(token, 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        wordMap.forEach((key, value) -> write(key, value, context));
    }

    private static void write(String key, Integer value, Context context)  {
        System.out.println("Key: " + key + ", value: " + value);
        try {
            context.write(new Text(key), new IntWritable(value));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
