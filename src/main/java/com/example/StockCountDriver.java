package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        //Get all stocks'sums counted
        Job job1 = Job.getInstance(conf, "Stock Count");
        job1.setJarByClass(StockCountDriver.class);
        job1.setMapperClass(StockCountMapper.class);
        job1.setReducerClass(StockCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            System.exit(-1);
        }

        //Sorted
        Job job2 = Job.getInstance(conf, "Sort stocks");
        job2.setJarByClass(StockCountDriver.class);
        job2.setMapperClass(SortCountMapper.class);
        job2.setReducerClass(SortCountReducer.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (!job2.waitForCompletion(true)) {
            System.exit(-1);
        }
    }
}
