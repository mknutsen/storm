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

import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroTweetBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	public static final String HASHTAG_STREAM = "hashtags";
	public static final String POPULAR_USERS_STREAM = "pop_users";
	public static final String TWEETBEAN_STREAM = "tweetbeans";

	// To decode Twitter's createdAt string to a java.util.Date object
	private SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

	protected static final Logger LOG = LoggerFactory.getLogger(AvroTweetBolt.class);
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// TODO any initialization steps go here
	}

	@Override
	public void execute(Tuple input) {

		// TODO Get decode your Avro object from the input field

		// TODO Emit each hashtag to the HASHTAG_STREAM, one hashtag per tuple

		// TODO Emit a tuple of (screen name, num followers) to the
		// POPULAR_USERS_STREAM

		// TODO Emit a tuple of (TweetBean, created_at time (as long integer))
		// to TWEETBEAN_STREAM

		// TODO ack the input tuple via collector
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO declare the output schemas of ALL THREE streams
	}
}