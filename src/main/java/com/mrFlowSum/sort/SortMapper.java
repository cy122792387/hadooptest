package com.mrFlowSum.sort;

import com.mrFlowSum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

  //拿到一行 FlowSumReducer输出的数据 , 切分出各个字段封装成 flowbean输出
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] fields = StringUtils.split(line, "\t");
    context.write(
        new FlowBean(fields[0], Long.parseLong(fields[1]), Long.parseLong(fields[2])),
        NullWritable.get()
    );
  }
}
