package com.org.mapReduce.flowCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        for(FlowBean value : values){
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }
        flowBean.setUpFlow(totalUp);
        flowBean.setDownFlow(totalDown);
        flowBean.setSumFlow();

        context.write(key,flowBean);
    }
}
