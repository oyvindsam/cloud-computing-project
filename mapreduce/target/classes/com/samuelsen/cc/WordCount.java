package com.samuelsen.cc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// hello
public class WordCount {
    public static void main(String[] args) throws Exception {
        // Get paths
        String inputPath = args[0];
        String outPath = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setPartitionerClass(WordPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path countPath = new Path(outPath + "/count");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, countPath);
        job.waitForCompletion(true);

        // Sort output from first MapReduce job
        conf = new Configuration();
        job = Job.getInstance(conf, "WordSort");
        FileInputFormat.addInputPath(job, countPath);
        FileOutputFormat.setOutputPath(job, new Path(outPath + "/sort"));
        job.setNumReduceTasks(1);  // to force all reduce tasks to
        job.setJarByClass(WordCount.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(WordKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }
}
