package com.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class HdfsUtil {
  FileSystem fs = null;

  @Before
  public void before() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    fs = FileSystem.get(conf);
  }

  @After
  public void after() throws IOException {
    fs.close();
  }

  @Test
  public void upLoad0() throws IOException {
    FSDataOutputStream out = fs.create(new Path("hdfs://localhost:9000/test/localfile0"));
    FileInputStream in = new FileInputStream("/home/cy/test/localfile");
    IOUtils.copy(in, out);
  }

  @Test
  public void upLoad() throws IOException {
    fs.copyFromLocalFile(new Path("/home/cy/test/localfile"), new Path("hdfs://localhost:9000/test"));
  }

  @Test
  public void downLoad() throws IOException {
    fs.copyToLocalFile(new Path("hdfs://localhost:9000/test/hdfsfile"), new Path("/home/cy/test/"));
  }

  @Test
  public void listFiles() throws IOException {
    // listFiles列出的是文件信息，而且提供递归遍历
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

    while(files.hasNext()){

      LocatedFileStatus file = files.next();
      Path filePath = file.getPath();
      String fileName = filePath.getName();
      System.out.println(fileName);

    }

    System.out.println("---------------------------------");

    //listStatus 可以列出文件和文件夹的信息，但是不提供自带的递归遍历
    FileStatus[] listStatus = fs.listStatus(new Path("/"));
    for(FileStatus status: listStatus){

      String name = status.getPath().getName();
      System.out.println(name + (status.isDirectory()?" is dir":" is file"));

    }
  }

  @Test
  public void mkdir() throws IOException {
    fs.mkdirs(new Path("hdfs://localhost:9000/test/mkdirfile"));
  }

  @Test
  public void delete() throws IOException {
    fs.delete(new Path("hdfs://localhost:9000/test/mkdirfile"), true);
  }


}
