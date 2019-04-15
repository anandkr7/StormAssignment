package com.storm.assignment.driver;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import com.storm.assignment.bolt.SplitSentenceBolt;
import com.storm.assignment.bolt.WordCountBolt;
import com.storm.assignment.spout.SentenceSpout;
import com.storm.assignment.util.Utils;

/**
 * @author Anand WordCountTopology code which will act as the driver class and
 *         configure the topology
 */
public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {

		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);

		// SentenceSpout --> SplitSentenceBolt
		// Using the Shuffle Grouping from SentenceSpout to randomly distribute tuple to
		// SplitSentence Bolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 3).shuffleGrouping(SENTENCE_SPOUT_ID);

		// SplitSentenceBolt --> WordCountBolt
		// Changing the Fields Grouping to Shuffle Grouping to randomly route the
		// tuples to all the WordCount Bolts to solve the load imbalance problem with
		// fields grouping.
		builder.setBolt(COUNT_BOLT_ID, countBolt, 4).shuffleGrouping(SPLIT_BOLT_ID);

		Config config = new Config();
		if (args != null && args.length > 0) {
			// Production Mode
			config.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		} else {
			// Local Mode
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
			Utils.waitForSeconds(20);
			cluster.shutdown();
		}
	}
}
