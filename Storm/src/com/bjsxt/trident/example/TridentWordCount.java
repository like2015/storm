package com.bjsxt.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;


public class TridentWordCount {
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static StormTopology buildTopology(LocalDRPC drpc) {
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
        new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
        new Values("how many apples can you eat"), new Values("to be or not to be the person"));
    spout.setCycle(true);

    TridentTopology topology = new TridentTopology();
    
    // 下面这一段是来构建实时计算时候的DAG，这个程序不会停，TridentState状态供实时来进行查询！
    TridentState wordCounts = topology.newStream("spout1", spout) // 开始接入数据源
    		.parallelismHint(16) // 开启这个接收数据源的并行度
    		// 把上游的每条数据sentence取到交给逻辑Split()来进行计算，后面这个word相当于之前的declarerOutputFields()
    		.each(new Fields("sentence"), new Split(), new Fields("word"))
    		// 按照词来进行分组
    		.groupBy(new Fields("word"))
    		// 第一个参数是把聚合的结果存哪里！第二个参数是把上面分好组的每一组按照什么方式聚合！最后count也就是存在数据库里面的名称叫什么！
    		// MemoryMapState你可以理解为就是在内存里面创建一个HashMap来记录数据！
    		.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
    		// 这一层处理它的并行度是多少
    		.parallelismHint(16);

    // 下面这一段就是即时查询部分，以及计算逻辑，叫DRPCStream其实说白了就是会用到DRPCSpout
    topology.newDRPCStream("words", drpc)
    // each就是对每个发过来的请求都来进行处理！
    .each(new Fields("args"), new Split(), new Fields("word"))
    // 按照词分组了
    .groupBy(new Fields("word"))
    // stateQuery就是去TridentState里面去查wordCounts是上面的State，
    // 第二个参数new Fields("word")就相当于拿什么key去TridentState去查询
    // 第三个参数new MapGet()就是查询的方式
    // 第四个参数new Fields("count")是把查询回来的结果命名为什么名称，供下面再去算
    .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
    // 就是把返回为Null空的过滤掉
    .each(new Fields("count"),new FilterNull())
    // sum会把值全部累加
    .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        System.err.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
        Thread.sleep(1000);
      }
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
