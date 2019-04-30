package demo.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer implements IRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * *bolt*从单词文件接收到文本行，并标准化它。 文本行会全部转化成小写，并切分它，从中得到所有单词。
	 */
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				// 发布这个单词
				collector.emit(new Values(word));
			}
		}
		// 对元组做出应答
		collector.ack(input);

	}

	public void cleanup() {
		// TODO Auto-generated method stub

	}

	/**
	 * 这个*bolt*只会发布“word”域
	 * 
	 * 首先我们要声明bolt的出参：
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 这里我们声明bolt将发布一个名为“word”的域。
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
