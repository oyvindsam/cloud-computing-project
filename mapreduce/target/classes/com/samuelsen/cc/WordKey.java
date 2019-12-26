package com.samuelsen.cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordKey implements WritableComparable<WordKey> {

    private Text word;
    private IntWritable count;

    public WordKey() {
        super();
    }

    public WordKey(Text word, IntWritable count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public int compareTo(WordKey wordKey) {
        int compare = count.compareTo(wordKey.count);
        return compare != 0 ? -compare : -word.compareTo(wordKey.word);
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getCount() {
        return count;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = new Text(Text.readString(dataInput));
        this.count = new IntWritable(dataInput.readInt());
    }
}
