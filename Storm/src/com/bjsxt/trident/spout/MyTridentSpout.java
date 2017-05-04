package com.bjsxt.trident.spout;


import java.util.Map;
import java.util.Random;

import com.bjsxt.basic.transactional.MyMeta;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MyTridentSpout implements ITridentSpout<MyMeta>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public storm.trident.spout.ITridentSpout.BatchCoordinator<MyMeta> getCoordinator(
			String txStateId, Map conf, TopologyContext context) {
		return new MyBatchCoordinator();
	}

	@Override
	public storm.trident.spout.ITridentSpout.Emitter<MyMeta> getEmitter(
			String txStateId, Map conf, TopologyContext context) {
		return new MyEmitter();
	}

	@Override
	public Fields getOutputFields() {
		return null;
	}
	
	public class MyBatchCoordinator implements BatchCoordinator<MyMeta>
	{

		@Override
		public void close() {
			
		}

		@Override
		public MyMeta initializeTransaction(long txid, MyMeta prevMetadata,
				MyMeta currMetadata) {
			return null;
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public void success(long txid) {
			
		}
		
	}
	
	public class MyEmitter implements Emitter<MyMeta>
	{

		@Override
		public void close() {
			
		}

		@Override
		public void emitBatch(TransactionAttempt tx, MyMeta coordinatorMeta,
				TridentCollector collector) {
			Random random = new Random();
			String[] hosts = { "www.taobao.com" };
			String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
					"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
			String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
					"2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
			
			for (long i = 0; i < 100; i++) {
				collector.emit(new Values(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]));
			}
		}

		@Override
		public void success(TransactionAttempt tx) {
			
		}
		
	}

}
