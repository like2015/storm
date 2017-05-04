package com.bjsxt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Test20170222 {
	

	public static void main(String[] agrs){
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MySpout(), 1);
		builder.setBolt("bolt", new MyBolt() ,2).shuffleGrouping("spout");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Test20170222", conf, builder.createTopology());
	}
}
