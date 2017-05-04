package com.bjsxt.basic.transactional;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMeta>{

	Map<Long, String> dbMap  = null;
	public MyEmitter(Map<Long, String> dbMap) {
		this.dbMap = dbMap;
	}
	
	@Override
	public void cleanupBefore(BigInteger txid) {
		
	}

	@Override
	public void close() {
		
	}

	@Override
	public void emitBatch(TransactionAttempt tx, MyMeta coordinatorMeta,
			BatchOutputCollector collector) {
		
		long beginPoint = coordinatorMeta.getBeginPoint() ;
		int num = coordinatorMeta.getNum() ;
		
		for (long i = beginPoint; i < num+beginPoint; i++) {
			if (dbMap.get(i)==null) {
				continue;
			}
			collector.emit(new Values(tx,dbMap.get(i)));
		}
	}

}
