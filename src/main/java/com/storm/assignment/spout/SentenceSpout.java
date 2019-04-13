package com.storm.assignment.spout;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.storm.assignment.util.Utils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -3265801584230264636L;
	private ConcurrentHashMap<UUID, Values> pending;
	private SpoutOutputCollector collector;
	private String[] sentences = { "the cow jumped over the moon" };
	private int index = 0;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

	public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();
	}

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

	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}

	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId), msgId);
	}
}
