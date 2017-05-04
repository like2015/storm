package com.bjsxt.basic.transactional.opaque;


import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.bjsxt.basic.transactional.MyMeta;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class MyOpaquePtTxSpout implements IOpaquePartitionedTransactionalSpout<MyMeta>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static int BATCH_NUM = 10 ;
	public Map<Integer, Map<Long,String>> PT_DATA_MP = new HashMap<Integer, Map<Long,String>>();
	
	public MyOpaquePtTxSpout()
	{
		Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2017-02-21 08:40:50", "2017-02-21 08:40:51", "2017-02-21 08:40:52", "2017-02-21 08:40:53", 
				"2017-02-21 09:40:49", "2017-02-21 10:40:49", "2017-02-21 11:40:49", "2017-02-21 12:40:49" };
		
		for (int j = 0; j < 5; j++) {
			HashMap<Long, String> dbMap = new HashMap<Long, String> ();
			for (long i = 0; i < 100; i++) {
				dbMap.put(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]);
			}
			PT_DATA_MP.put(j, dbMap);
		}
	}
	
	@Override
	public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Coordinator getCoordinator(
			Map conf, TopologyContext context) {
		return new MyCoordinator();
	}

	@Override
	public backtype.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout.Emitter<MyMeta> getEmitter(
			Map conf, TopologyContext context) {
		return new MyEmitter();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public class MyCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator
	{

		@Override
		public void close() {
			
		}

		@Override
		public boolean isReady() {
			Utils.sleep(1000);
			return true;
		}
		
	}
	public class MyEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<MyMeta>
	{

		@Override
		public void close() {
			
		}

		@Override
		public MyMeta emitPartitionBatch(TransactionAttempt tx,
				BatchOutputCollector collector, int partition,
				MyMeta lastPartitionMeta) {
			System.err.println("emitPartitionBatch partition:"+partition);
			long beginPoint = 0;
			if (lastPartitionMeta == null) {
				beginPoint = 0 ;
			}else {
				beginPoint = lastPartitionMeta.getBeginPoint() + lastPartitionMeta.getNum() ;
			}
			
			MyMeta meta = new MyMeta() ;
			meta.setBeginPoint(beginPoint);
			meta.setNum(BATCH_NUM);
			System.err.println("启动一个事务："+meta.toString());
//			emitPartitionBatch(tx,collector,partition,meta);
			Map<Long, String> batchMap = PT_DATA_MP.get(partition);
			for (Long i = meta.getBeginPoint(); i < meta.getBeginPoint()+meta.getNum(); i++) {
				if (batchMap.size()<=i) {
					break;
				}
				collector.emit(new Values(tx,batchMap.get(i)));
			}
			return meta;
		}

		@Override
		public int numPartitions() {
			return 5;
		}
	}
}
