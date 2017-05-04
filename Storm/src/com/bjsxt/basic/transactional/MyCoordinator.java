package com.bjsxt.basic.transactional;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMeta>{

	public static int BATCH_NUM = 10 ;
	
	@Override
	public void close() {
		
	}

	@Override
	public MyMeta initializeTransaction(BigInteger txid, MyMeta prevMetadata) {
		long beginPoint = 0;
		if (prevMetadata == null) {
			beginPoint = 0 ;
		}else {
			beginPoint = prevMetadata.getBeginPoint() + prevMetadata.getNum() ;
		}
		
		MyMeta meta = new MyMeta() ;
		meta.setBeginPoint(beginPoint);
		meta.setNum(BATCH_NUM);
		System.err.println("启动一个事务："+meta.toString());
		return meta;
	}

	@Override
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

}
