package com.mrFlowSum.areaPartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPartitioner<K, V> extends Partitioner<K, V> {

  private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();

  static {
    areaMap.put("13", 0);
    areaMap.put("18", 1);
    areaMap.put("17", 2);
  }

  public int getPartition(K k, V v, int i) {
    int areaCode = areaMap.get(k.toString().substring(0, 2) == null ? 3 : k.toString().substring(0, 2));
    return areaCode;
  }

}
