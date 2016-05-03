package com.adamjshook.demo.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.bolt.PrinterBolt;
import org.apache.storm.topology.TopologyBuilder;

public class Driver {

    public static final String TWEET_STREAM = "tweet_stream";

    // TODO is this the name of your Kafka tweet topic?
    private static final String TWEET_TOPIC = "tweets";
 
    private static final String HASHTAG_KEY = "hashtags";

    private static final String POPUSERS_KEY = "popusers";

    private static final String TWEETBEAN_KEY = "tweetbeans";

    public static void main(String[] args) throws Exception {
        Config config = getConfig();
        StormTopology topology = getTopology();

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, topology);
            Thread.sleep(30000);
        } else {
            StormSubmitter.submitTopology(args[0], config, topology);
        }
        System.exit(0);
    }

    protected static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    public static StormTopology getTopology() {
        final TopologyBuilder tp = new TopologyBuilder();

        // TODO Build topology
        tp.setSpout("spout_tweets", new KafkaSpout("localhost", 9092, TWEET_TOPIC, TWEET_STREAM));
        tp.setBolt("printer", new PrinterBolt()).shuffleGrouping("spout_tweets",TWEET_STREAM);
        return tp.createTopology();
    }
}