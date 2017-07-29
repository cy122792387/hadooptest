package com.mrFlowSum.sort;

import com.mrFlowSum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortMR {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(SortMR.class);

    job.setMapperClass(SortMapper.class);
    job.setReducerClass(SortReducer.class);

    job.setMapOutputKeyClass(FlowBean.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);

//    FileInputFormat.setInputPaths(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/mr/flow/output/part-r-00000"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/mr/flow/sortOutput"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}