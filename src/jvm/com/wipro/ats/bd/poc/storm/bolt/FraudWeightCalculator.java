
package com.wipro.ats.bd.poc.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class FraudWeightCalculator extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //"cardNum","proc","txnc","amt"
    int _fraudWeight =1;
    String profileCountry = (String)tuple.getValueByField("proc");
    String txnCountry = (String)tuple.getValueByField("txnc");
    Double txnAmount = (Double) tuple.getValueByField("amt");
    if(!profileCountry.equals(txnCountry)) {

      if(profileCountry.equalsIgnoreCase("Belarush"))
      {
        _fraudWeight+=10;
      }
      else if(profileCountry.equalsIgnoreCase("Ghana"))
      {
        _fraudWeight+=5;
      }
      else
      {
        _fraudWeight+=2;
      }
    }
    if(txnAmount.intValue() > 500 && txnAmount.intValue() <= 1000)
    {
      _fraudWeight*=2;
    }
    else if(txnAmount.intValue() > 1000)
    {
      _fraudWeight*=4;
    }
    collector.emit(new Values(tuple.getValueByField("cardNum"),profileCountry,txnCountry,txnAmount,_fraudWeight));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cardNum","proc","txnc","amt","fraudw"));
  }

}
