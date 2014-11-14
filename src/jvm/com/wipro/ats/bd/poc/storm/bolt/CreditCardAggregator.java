
package com.wipro.ats.bd.poc.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;


public class CreditCardAggregator extends BaseBasicBolt {
  HTable _table = null;
  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    Configuration conf = HBaseConfiguration.create();
    try {

      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("creditcard"));
      tableDescriptor.addFamily(new HColumnDescriptor("txlog"));
      admin.createTable(tableDescriptor);
    } catch (TableExistsException e) {
      System.out.println("Table exists. Moving on");
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      if(_table==null) {
        _table = new HTable(conf, "creditcard");
      }
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {

    Integer fraudw= (Integer) tuple.getValueByField("fraudw");
    if(fraudw > 12) {

      try {

        String card = tuple.getValueByField("cardNum").toString();
        Put put = new Put(Bytes.toBytes(card));
        put.add(Bytes.toBytes("txlog"), Bytes.toBytes("Profile Country"), Bytes.toBytes(tuple.getValueByField("proc").toString()));
        put.add(Bytes.toBytes("txlog"), Bytes.toBytes("Txn Country"), Bytes.toBytes(tuple.getValueByField("txnc").toString()));
        put.add(Bytes.toBytes("txlog"), Bytes.toBytes("Amount"), Bytes.toBytes(tuple.getValueByField("amt").toString()));
        put.add(Bytes.toBytes("txlog"), Bytes.toBytes("Fraud Weight"), Bytes.toBytes(tuple.getValueByField("fraudw").toString()));
        _table.put(put);
        _table.flushCommits();

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("cardNum","proc","txnc","amt","fraudw"));
  }

  @Override
  public void cleanup() {
    super.cleanup();
    try {
      _table.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    ;
  }
}
