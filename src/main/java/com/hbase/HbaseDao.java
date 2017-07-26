package com.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HbaseDao {
  Configuration conf = null;
  String tablename = null;
  HBaseAdmin admin = null;
  HTable table = null;

  @Before
  public void before() throws IOException {
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
    tablename = "nvshen";
    admin = new HBaseAdmin(conf);
    table = new HTable(conf, tablename);
  }

  @After
  public void after() throws IOException {
    admin.close();
    table.close();
  }

  @Test
  public void createTable() throws IOException {
    TableName name = TableName.valueOf("nvshen");
    HTableDescriptor desc = new HTableDescriptor(name);
    HColumnDescriptor base_info = new HColumnDescriptor("base_info");
    base_info.setMaxVersions(5);
    HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
    extra_info.setMaxVersions(5);
    desc.addFamily(base_info);
    desc.addFamily(extra_info);
    admin.createTable(desc);
  }

  @Test
  public void put() throws IOException {
    Put put = new Put(Bytes.toBytes("1"));
    put.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("angelababy"));
    put.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("28"));
    table.put(put);
  }

  @Test
  public void get() throws IOException {
    Get get = new Get(Bytes.toBytes("1"));
    get.setMaxVersions(5);
    Result res = table.get(get);
    List<Cell> cellList = res.listCells();
    for (Cell cell : cellList) {
      String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
      long timestamp = cell.getTimestamp();  //取到时间戳
      String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
      String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
      String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

      System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +
          timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
    }
  }

  @Test
  public void scan() {
    //加多种filter
  }

  @Test
  public void drop() throws IOException {
    admin.disableTable(tablename);
    admin.deleteTable(tablename);
  }

  public static void main(String[] args) throws IOException {

  }

}
