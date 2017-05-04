package com.bjsxt.basic.pv.simple;


import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PVBoltSimple implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String logString = null;
	String session_id = null;
	long Pv = 0;
	
	@Override
	public void cleanup() {
		
	}
	
	@Override
	public void execute(Tuple input) {
		logString = input.getString(0);
		session_id = logString.split("\t")[1];
		if (session_id != null) {
			Pv ++ ;
		}
		
		System.err.println("pv = "+ Pv * 2);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
