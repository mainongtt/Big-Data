package com.org.mapReduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean flowBean = new FlowBean();
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");

        String phone = split[0];
        Long upFlow = Long.parseLong(split[1]);
        Long downFlow = Long.parseLong(split[2]);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpFlow(upFlow);
        flowBean.setSumFlow();

        context.write(flowBean,v);
    }
}
