package com.adamjshook.demo.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.codehaus.jackson.map.ObjectMapper;

import redis.clients.jedis.Jedis;

@SuppressWarnings("rawtypes")
public class RedisSetTweetBeanBolt extends BaseRichBolt {
	private static final long serialVersionUID = 373615422224513756L;
	private String key;
	private String host;
	private int port;
	private OutputCollector collector;

	private transient ObjectMapper mapper;
	private transient Jedis jedis;

	public RedisSetTweetBeanBolt(String key, String host, int port) {
		// TODO set member variables
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		// TODO initialize jedis and mapper
	}

	@Override
	public void execute(Tuple input) {
		// TODO Convert TweetBean to JSON String using ObjectMapper

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
