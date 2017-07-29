package com.mrFlowSum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*FlowBean是我们自定义的一个数据类型,要在hadoop的各个节点之间传输,应该遵循 hadoop的序列化机制
  就必须实现hadoop相应的序列化接口Writable
*
* */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
  //拿到日志中的一行数据, 切分各个字段, 抽取出我们需要的字段: 手机号,上行流量,下行流量,然后封装成kv 发送出去
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //key表示行标 value就是这行的数据
    String line = value.toString();
    String[] fields = line.split(" ");
    String phoneNB = fields[0];
    long up_flow = Long.parseLong(fields[1]);
    long d_flow = Long.parseLong(fields[2]);

    //封装成 kv 并输出
    context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow, d_flow));
  }
}
