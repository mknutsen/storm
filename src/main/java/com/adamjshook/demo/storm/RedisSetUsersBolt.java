package com.adamjshook.demo.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import redis.clients.jedis.Jedis;

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
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		// TODO Initialize Jedis
	}

	@Override
	public void execute(Tuple input) {
		// TODO Write to Jedis

		// TODO ack input tuple
	}

	@Override
	public void cleanup() {
		// TODO close jedis
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
