package com.org.mapReduce.elt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeblogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        boolean result = Parse(lines);
        outK.set(lines);
        if (result) {
            context.write(outK, NullWritable.get());
        } else {
            return;
        }
    }

    private boolean Parse(String lines) {
        String[] split = lines.split(" ");
        if (split.length >= 11) {
            return true;
        } else {
            return false;
        }
    }
}
