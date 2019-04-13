package com.storm.assignment.driver;

import com.storm.assignment.bolt.DatabaseServiceBolt;
import com.storm.assignment.bolt.ReportBolt;
import com.storm.assignment.bolt.SplitSentenceBolt;
import com.storm.assignment.bolt.WordCountBolt;
import com.storm.assignment.spout.SentenceSpout;
import com.storm.assignment.util.Utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String MYSQL_DATABASE_BOLT_ID = "mysql_database_bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {

        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        DatabaseServiceBolt databaseServiceBolt = new DatabaseServiceBolt();
        
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);
        
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt, 3)
                .setNumTasks(4)
                .shuffleGrouping(SENTENCE_SPOUT_ID);
        
        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt, 4)
        		.setNumTasks(4)
        		.shuffleGrouping(SPLIT_BOLT_ID);
        
        // WordCountBolt --> DatabaseServiceBolt
        builder.setBolt(MYSQL_DATABASE_BOLT_ID, databaseServiceBolt)
                .globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
