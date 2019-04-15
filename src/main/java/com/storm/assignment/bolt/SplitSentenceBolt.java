package com.storm.assignment.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author Anand SplitSentence Bolt which will taks the input tuples from
 *         SentenceSpout and split the words in the sentences and send to the
 *         further bolts
 */
public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1608666534218867519L;
	private OutputCollector collector;

	// Initialize method for the Bolt to initialize Collector
	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	// Method which will process the incoming tuple from SentenceSpout and split the
	// words and emit the same with tuple identifier for Anchoring
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for (String word : words) {
			this.collector.emit(tuple, new Values(word));
		}
		this.collector.ack(tuple);
	}

	// Declaration of Fields
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}
