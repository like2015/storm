package com.bjsxt.basic.wc.interval;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SumBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {

	}
	long beginTime = System.currentTimeMillis() ;
	long endTime = 0;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			endTime = System.currentTimeMillis() ;
			if (endTime - beginTime >= 5 * 1000) {
				Map<String, Integer> counts = (Map<String, Integer>) input.getValue(0);
				long word_sum = 0;// 总数
				long word_count = 0; // 个数，去重后
				//获取总数，遍历counts 的values，进行sum
				Iterator<Integer> i = counts.values().iterator() ;
				while(i.hasNext())
				{
					word_sum += i.next();
				}
				
				//获取word去重个数，遍历counts 的keySet，取count
				Iterator<String> i2 = counts.keySet().iterator() ;
				while(i2.hasNext())
				{
					String oneWordString = i2.next();
					if (oneWordString != null) {
						word_count ++ ;
					}
				}
				
				System.err.println("word_sum="+word_sum+";  word_count="+word_count);
				beginTime = System.currentTimeMillis() ;
			}
		} catch (Exception e) {
			throw new FailedException("SumBolt fail!");
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
