package com.org.mapReduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ParitionProvice extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phone = text.toString();
        int res = -1;
        if(phone.startsWith("136")){
            res = 0;
        }else if(phone.startsWith("137")){
            res = 1;
        }else if(phone.startsWith("138")){
            res = 2;
        }else if(phone.startsWith("139")){
            res = 3;
        }else{
            res = 4;
        }
        return res;
    }
}
