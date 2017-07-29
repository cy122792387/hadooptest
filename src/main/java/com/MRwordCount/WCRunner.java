package com.MRwordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
* 用来描述一个特定的作业
 * 包括,该作业使用哪个map类, 哪个reduce 类
 * 还可以指定该作业要处理的数据所在的路径
 * 也可以指定该作业 输出的结果放到哪个路径
*/
public class WCRunner {

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job wcjob = Job.getInstance(conf);

    //设置job所用的类在哪个jar包
    wcjob.setJarByClass(WCRunner.class);

    //指定map 和reduce所用的类
    wcjob.setMapperClass(WCMapper.class);
    wcjob.setReducerClass(WCReducer.class);

    //指定map输出的k v 数据类型
    wcjob.setMapOutputKeyClass(Text.class);
    wcjob.setMapOutputValueClass(LongWritable.class);

    //指定reduce输出的k v 数据类型
    wcjob.setOutputKeyClass(Text.class);
    wcjob.setOutputValueClass(LongWritable.class);

    //指定原数据路径 和 输出数据路径
    FileInputFormat.setInputPaths(wcjob, new Path("hdfs://localhost:9000/mr/wordCount/input"));
    FileOutputFormat.setOutputPath(wcjob, new Path("hdfs://localhost:9000/mr/wordCount/output1"));

    wcjob.waitForCompletion(true);
  }

}
