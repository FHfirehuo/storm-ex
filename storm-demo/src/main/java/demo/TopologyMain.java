package demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import demo.bolts.WordCounter;
import demo.bolts.WordNormalizer;
import demo.spouts.WordReader;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// 定义拓扑
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

		// 配置
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		// 运行拓扑
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		conf.setDebug(true);
		
		//正如之前讲过的，你要用一个LocalCluster对象运行这个拓扑。在生产环境中，拓扑会持续运行，不过对于这个例子而言，你只要运行它几秒钟就能看到结果。
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
		Thread.sleep(2000);
		cluster.shutdown();
	}
}
