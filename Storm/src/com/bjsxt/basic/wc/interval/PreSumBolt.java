package com.bjsxt.basic.wc.interval;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PreSumBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;
	Map<String, Integer> counts = new HashMap<String, Integer>();
	
	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			String word = input.getString(0);
			Integer count = input.getInteger(1);
			counts.put(word, count);
			collector.emit(new Values(counts));
			
		} catch (Exception e) {
			throw new FailedException("SumBolt fail!");
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("countMap"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
