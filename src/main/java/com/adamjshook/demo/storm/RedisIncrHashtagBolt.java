package com.adamjshook.demo.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class RedisIncrHashtagBolt extends BaseRichBolt {

    private static final long serialVersionUID = -4918241919840851689L;

    private String key;

    private String host;

    private int port;

    private transient Jedis jedis;

    private OutputCollector collector;

    public RedisIncrHashtagBolt(String key, String host, int port) {
        // TODO set member variables
        this.key = key;
        this.host = host;
        this.port = port;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // TODO initialize Jedis
        jedis = new Jedis(host, port);
    }

    @Override
    public void execute(Tuple input) {
        String hashtag = input.getString(0);
        int count = 0;
        try {
            count = Integer.parseInt(jedis.get(hashtag));
        } catch (NumberFormatException e) {
            count = 0;
        }
        // TODO Write stuff to jedis
        jedis.set(key, hashtag, "" + (count + 1));

        // TODO ack tuple
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
