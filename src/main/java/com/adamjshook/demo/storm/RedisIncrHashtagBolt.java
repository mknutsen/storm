package com.adamjshook.demo.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import redis.clients.jedis.Jedis;

public class RedisIncrHashtagBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4918241919840851689L;
	private String key;
	private String host;
	private int port;

	private transient Jedis jedis;
	private OutputCollector collector;

	public RedisIncrHashtagBolt(String key, String host, int port) {
		// TODO set member variables
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		// TODO initialize Jedis
	}

	@Override
	public void execute(Tuple input) {
		// TODO Write stuff to jedis

		// TODO ack tuple
	}

	@Override
	public void cleanup() {
		// TODO close jedis
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
