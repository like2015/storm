package com.bjsxt.basic.wc.interval;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new MyRandomSentenceSpout(), 1);

		builder.setBolt("split", new MySplitBolt(" "), 8).shuffleGrouping("spout");

		builder.setBolt("count", new WordCountBolt(), 3).fieldsGrouping("split",
				new Fields("word"));

		builder.setBolt("preSum", new PreSumBolt(), 3).shuffleGrouping("count");
		builder.setBolt("sum", new SumBolt(), 1).shuffleGrouping("preSum");
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());

			// Thread.sleep(10000);
			// cluster.shutdown();
		}
	}
}
