package com.storm.assignment.spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.storm.assignment.util.Utils;

/**
 * @author Anand Sentence Spout which will act as the Source of the tuple words
 */
public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -3265801584230264636L;
	private ConcurrentHashMap<UUID, Values> pending;
	private SpoutOutputCollector collector;

	// Message which acts as source
	private String[] sentences = { "my dog has fleas", "i like cold beverages", "the dog ate my homework",
			"dont have a cow man", "i dont think i like fleas" };
	private int index = 0;

	// Declaration of Fields
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	// Initialize method for the Spout to initialize Collector and Pending HashMap
	public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();
	}

	// Method which will Emit the tuples along with the Message Identifiers
	public void nextTuple() {
		Values values = new Values(sentences[index]);
		UUID msgId = UUID.randomUUID();
		this.pending.put(msgId, values);
		this.collector.emit(values, msgId);
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.waitForMillis(1);
	}

	// Method to Acknowledge the tuple with message ids and removing from the
	// Pending HashMap
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}

	// Method to Re-Emit the tuple for the Message id once the failure is
	// encountered
	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId), msgId);
	}
}
