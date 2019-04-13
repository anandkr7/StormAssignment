package com.storm.assignment.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.upgrad.assignment.mysql.services.DatabaseService;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class DatabaseServiceBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private static DatabaseService databaseService;
	private HashMap<String, Long> counts = null;
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		databaseService = new DatabaseService("localhost", "storm_assignment", "root", "Admin@123");
		counts = new HashMap<String, Long>();
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		if (tuple != null) {
			String word = tuple.getStringByField("word");
			Long count = tuple.getLongByField("count");
			this.counts.put(word, count);
		}
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
			System.out.println(key + " : " + this.counts.get(key));
			databaseService.persist(key, this.counts.get(key));
		}
		System.out.println("--------------");
	}

	public static void main(String[] args) {

		databaseService = new DatabaseService("localhost", "storm_assignment", "root", "Admin@123");
		databaseService.persist("Assignment", 100l);

	}

}