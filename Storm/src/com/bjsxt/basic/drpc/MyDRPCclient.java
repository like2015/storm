package com.bjsxt.basic.drpc;


import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class MyDRPCclient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		DRPCClient client = new DRPCClient("node1", 3772);
		try {
			String result = client.execute("exclamation", "hello ");
			
			System.out.println(result);
		} catch (TException e) {
			e.printStackTrace();
		} catch (DRPCExecutionException e) {
			e.printStackTrace();
		} 
	}
}
