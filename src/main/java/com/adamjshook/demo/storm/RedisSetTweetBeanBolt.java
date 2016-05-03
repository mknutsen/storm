package com.adamjshook.demo.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Map;

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
        this.key = key;
        this.host = host;
        this.port = port;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // TODO initialize jedis and mapper
        mapper = new ObjectMapper();
        jedis = new Jedis(host, port);

    }

    @Override
    public void execute(Tuple input) {
        // TODO Convert TweetBean to JSON String using ObjectMapper
        try {

            TweetBean bean = (TweetBean) input.getValue(0);
            String tweetJson = mapper.writeValueAsString(bean);


            // TODO Write stuff to jedis
            jedis.set(key, bean.getCreated().toString(), tweetJson);
            
            // TODO ack tuple
            collector.ack(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
