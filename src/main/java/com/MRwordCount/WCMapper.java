package com.MRwordCount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//4个泛型,前2个是map输入数据类型KEYIN KEYOUT,后两个是map输出数据类型KEYOUT VALUEOUT
//默认情况下,框架传递给我们的输入数据中key是要处理文本中一行的起始偏移量,这一行的内容作为value
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

  //mapreduce 每读取一行就调用一次该方法
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    //具体业务逻辑就写在这个方法体中 ,而且我们业务要处理的数据已经被框架传递出来 ,在方法参数的key-value中
    //key是这一行数据的起始偏移量 ,value是这一行的文本内容
    String line = value.toString();
    String[] words = StringUtils.split(line, " ");

    //由context负责输出数据到reduce中
    for (String word : words) {
      //输出的k为单词 ,value为1
      context.write(new Text(word), new LongWritable(1));
    }
  }

}
