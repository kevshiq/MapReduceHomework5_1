package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVParser;

import java.io.IOException;

public class StockCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable one = new LongWritable(1);
    private Text stockSymbol = new Text();
    private CSVParser csvParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String Line = value.toString();
        String[] stockLine = csvParser.parseLine(Line);

        if (stockLine.length >= 4) {
            String stockName = stockLine[3].trim();
            if (!stockName.isEmpty()) {
                stockSymbol.set(stockName);
                context.write(stockSymbol, one);
            }
        }
    }
}