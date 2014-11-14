
package com.wipro.ats.bd.poc.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.*;

public class CreditLogGeneratorSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  List<String> _countryList;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    _countryList = new ArrayList<String>();
    String[] locales = Locale.getISOCountries();
    for (String countryCode : locales) {
      Locale obj = new Locale("", countryCode);
      _countryList.add(obj.getDisplayCountry());

    }
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    String profileCountry = _countryList.get(_rand.nextInt(_countryList.size()));
    String txnCountry = _rand.nextInt(10)==8 ? _countryList.get(_rand.nextInt(_countryList.size())):profileCountry;
    double txnAmount = _rand.nextDouble()*_rand.nextInt(5000);
    //Card number range
    long minimum = 1000546587382100l;
    long maximum = 1000546587382199l;
    long n =  maximum - minimum  + 1l;
    long i = Math.abs(_rand.nextLong() % n);
    long randomNum =  minimum + i;

    _collector.emit(new Values(randomNum,profileCountry,txnCountry,txnAmount));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cardNum","proc","txnc","amt"));
  }

}