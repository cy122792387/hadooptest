package com.MRwordCount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//输入是单词和 它的数量 ,输出是单词和统计数 ,这里就是相加即可
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  //map 框架在处理完成后,把所以的kv缓存起来 分组(相同的key为一组) 传递的时候 每次传给reduce一个组 key, values
  //这里第二个参数是 list of values 的 iterator
  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

    long count = 0;
    for (LongWritable value : values) {
      count += value.get();
    }
    //输出这一个单词的结果
    context.write(key, new LongWritable(count));
  }

}
