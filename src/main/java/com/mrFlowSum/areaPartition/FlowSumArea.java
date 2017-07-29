package com.mrFlowSum.areaPartition;

import com.mrFlowSum.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
对流量日志进行流量统计, 将不同省份的结果输出到不同的文件
需要自定义两个机制
 1,改造分区的逻辑, 自定义一个partitioner
 2,自定义reducer task 并发数
 */
public class FlowSumArea {

  public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split(" ");
      String phoneNB = fields[0];
      long up_flow = Long.parseLong(fields[1]);
      long d_flow = Long.parseLong(fields[2]);

      //封装成 kv 并输出
      context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow, d_flow));
    }
  }

  public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
      long up_flow_counter = 0;
      long d_flow_counter = 0;
      for (FlowBean bean : values) {
        up_flow_counter += bean.getUp_flow();
        d_flow_counter += bean.getD_flow();
      }
      context.write(key, new FlowBean(key.toString(), up_flow_counter, d_flow_counter));
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);

    job.setJarByClass(FlowSumArea.class);

    job.setMapperClass(FlowSumAreaMapper.class);
    job.setReducerClass(FlowSumAreaReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);

    job.setPartitionerClass(AreaPartitioner.class);
    //设置reduce任务数量, 和分组数保持一致
    job.setNumReduceTasks(4);
//    FileInputFormat.setInputPaths(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/mr/flow/input/flow.txt"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/mr/flow/partitionOutput1"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
