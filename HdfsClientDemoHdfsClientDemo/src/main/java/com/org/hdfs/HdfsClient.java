package com.org.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class HdfsClient {
    FileSystem fs;

    @Before
    public void init() throws Exception{
        URI uri = new URI("hdfs://hadoop100:8020");
        Configuration configuration = new Configuration();
        String user = "root";
        fs = FileSystem.get(uri, configuration, user);
    }

    @Test
    public void testRename() throws IOException {
        fs.rename(new Path("/input/words.txt"),new Path("/input/my_words.txt"));

    }

    @After
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void fileDetail() throws Exception{
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("==========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));
        }
    }
}
