package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortCountReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
    private long seq = 1;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            Text result = new Text(seq + "\t" + val.toString());
            context.write(result, key);
            seq += 1;
        }
    }
}
