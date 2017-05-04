package com.bjsxt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
//		String name = input.getString(0);
//		String lover = input.getString(1);
		String name = input.getStringByField("name");
		String lover = input.getStringByField("lover");
		System.err.println(Thread.currentThread().getName()+ "=====" + name + "=====" + lover);
		if("yasaka".equals(name)){
			collector.fail(input);
		}
//		collector.emit(new Values(name));
	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
