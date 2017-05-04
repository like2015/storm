package com.bjsxt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MySpout implements IRichSpout{

	private static final long serialVersionUID = 1L;

	SpoutOutputCollector collector = null;
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void close() {
		
	}

	@Override
	public void activate() {
		
	}

	@Override
	public void deactivate() {
		
	}

	@Override
	public void nextTuple() {
		try {
			collector.emit(new Values("yasaka","xuruyun"),new Values("yasaka","xuruyun"));
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(Object msgId) {
		System.err.println("Acked ====="+msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.err.println("Failed ===="+msgId);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name","lover"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	
}
