/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.adamjshook.demo.storm;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter.avro.Tweet;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class AvroTweetBolt extends BaseRichBolt {

    public static final String HASHTAG_STREAM = "hashtags";

    public static final String POPULAR_USERS_STREAM = "pop_users";

    public static final String TWEETBEAN_STREAM = "tweetbeans";

    protected static final Logger LOG = LoggerFactory.getLogger(AvroTweetBolt.class);

    private static final long serialVersionUID = 1L;

    // To decode Twitter's createdAt string to a java.util.Date object
    private SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

    private OutputCollector collector;

    private BinaryDecoder decoder;

    private SpecificDatumReader<Tweet> tweetBeanSpecificDatumReader;

    private Tweet tweet;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        // TODO any initialization steps go here
        tweetBeanSpecificDatumReader = new SpecificDatumReader<>(Tweet.class);
    }

    @Override
    public void execute(Tuple input) {
        try {
            // TODO Get decode your Avro object from the input field
            decoder = DecoderFactory.get().binaryDecoder(input.getBinary(0), decoder);
            tweet = tweetBeanSpecificDatumReader.read(tweet, decoder);

            // TODO Emit each hashtag to the HASHTAG_STREAM, one hashtag per tuple
            for (CharSequence hashtag : tweet.getHashtags()) {
                collector.emit(HASHTAG_STREAM, new Values(hashtag));
            }
            // TODO Emit a tuple of (screen name, num followers) to the
            // POPULAR_USERS_STREAM
            collector.emit(POPULAR_USERS_STREAM, new Values(tweet.getScreenName(), tweet.getFollowersCount()));

            // TODO Emit a tuple of (TweetBean, created_at time (as long integer))
            TweetBean bean = new TweetBean();
            bean.setCreated(formatter.parse(tweet.getCreatedAt().toString()));
            bean.setId(tweet.getId());
            bean.setTweet(tweet.getText().toString());
            bean.setUserId(tweet.getId());
            collector.emit(TWEETBEAN_STREAM, new Values(bean, bean.getCreated().getTime()));
            // TODO ack the input tuple via collector
            collector.ack(input);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO declare the output schemas of ALL THREE streams
        declarer.declareStream(HASHTAG_STREAM, new Fields("hashtag"));
        declarer.declareStream(POPULAR_USERS_STREAM, new Fields("screen_name", "num_followerst"));
        declarer.declareStream(TWEETBEAN_STREAM, new Fields("TweetBean", "created_at"));
    }
}