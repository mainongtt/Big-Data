package com.org.mapReduce.OutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class LogRecordWriter extends RecordWriter<Text, NullWritable> {


    private FSDataOutputStream fsdataoutputstream;
    private FSDataOutputStream dataOutputStream;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            fsdataoutputstream = fileSystem.create(new Path("D:\\toolsetup\\output\\atguigu.log"));
            dataOutputStream = fileSystem.create(new Path("\"D:\\\\toolsetup\\\\output\\\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if (log.contains("atguigu")) {
            fsdataoutputstream.writeBytes(log + "\n");
        } else {
            dataOutputStream.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fsdataoutputstream);
        IOUtils.closeStream(dataOutputStream);

    }
}
