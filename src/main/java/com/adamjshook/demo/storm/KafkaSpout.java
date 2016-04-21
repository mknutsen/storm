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

import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpout extends BaseRichSpout {
	private static final long serialVersionUID = 4452520061482019710L;
	protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
	private SpoutOutputCollector collector;

	private String host;
	private int port;
	private String topic;
	private String stream;

	// Transient objects are not serialized by Storm (you would get an error)
	private transient KafkaConsumer<String, byte[]> consumer;

	public KafkaSpout(String host, int port, String topic, String stream) {
		// TODO set member variables
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		// TODO Configure, create, and subscribe user to topics
	}

	@Override
	public void nextTuple() {
		// TODO Poll consumer for records, outputting to the correct topic
		// and a tuple of (key, value) of the record
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO declare output stream for each configured stream
	}
}