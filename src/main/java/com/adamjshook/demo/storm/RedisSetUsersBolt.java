package com.adamjshook.demo.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class RedisSetUsersBolt extends BaseRichBolt {

    private static final long serialVersionUID = 373615422224513756L;

    private String key;

    private String host;

    private int port;

    private transient Jedis jedis;

    private OutputCollector collector;

    public RedisSetUsersBolt(String key, String host, int port) {
        // TODO set member variables
        this.key = key;
        this.host = host;
        this.port = port;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // TODO Initialize Jedis
        jedis = new Jedis(host, port);
    }

    @Override
    public void execute(Tuple input) {
        String userName = input.getString(0);
        int followerCount = input.getInteger(1);

        // TODO Write to Jedis
        jedis.set(key, userName, followerCount + "");

        // TODO ack input tuple
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        // TODO close jedis
        jedis.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
