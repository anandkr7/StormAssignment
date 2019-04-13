package com.storm.assignment.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.storm.assignment.mysql.DatabaseService;

public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3646220662769984971L;
	private static DatabaseService databaseService;
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;

	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector) {
		databaseService = new DatabaseService("localhost", "storm_assignment", "root", "123");
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counts.put(word, count);
		this.collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// DO NOTHING
	}

	@Override
	public void cleanup() {
		System.out.println("--- FINAL COUNTS ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println("Saving data to database --- " + key + " : " + this.counts.get(key));
			databaseService.persist(key, this.counts.get(key));
		}
		System.out.println("--------------");
	}
}
