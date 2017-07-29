package com.mrFlowSum.sort;

import com.mrFlowSum.FlowBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean> {
  @Override
  protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    context.write(new Text(key.getPhoneNB()), key);
  }
}