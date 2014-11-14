
package com.wipro.ats.bd.poc.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.wipro.ats.bd.poc.storm.bolt.CreditCardAggregator;
import com.wipro.ats.bd.poc.storm.bolt.FraudWeightCalculator;
import com.wipro.ats.bd.poc.storm.bolt.PrinterBolt;
import com.wipro.ats.bd.poc.storm.spout.CreditLogGeneratorSpout;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class CreditLogTopology {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new CreditLogGeneratorSpout(), 5);
    //FraudWeightCalculator
    builder.setBolt("fraud", new FraudWeightCalculator(), 8).shuffleGrouping("spout");
    builder.setBolt("print", new PrinterBolt(), 8).shuffleGrouping("fraud");
    builder.setBolt("messenger", new CreditCardAggregator(), 12).fieldsGrouping("fraud", new Fields("cardNum"));

    Config conf = new Config();
    conf.setDebug(false);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("card-fraud", conf, builder.createTopology());

      //Thread.sleep(30000);

     // cluster.shutdown();
    }
  }
}
