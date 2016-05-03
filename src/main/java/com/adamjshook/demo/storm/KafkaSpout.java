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
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter.avro.Tweet;

import java.util.*;

public class KafkaSpout extends BaseRichSpout {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

    private static final long serialVersionUID = 4452520061482019710L;

    private SpoutOutputCollector collector;

    private String host;

    private int port;

    private String topic;

    private String stream;

    private boolean lock;

    private List<Object> tweetsToEmit;

    // Transient objects are not serialized by Storm (you would get an error)
    private transient KafkaConsumer<String, byte[]> consumer;

    private KafkaConsumerThread kafkaConsumerThread;

    public KafkaSpout(String host, int port, String topic, String stream) {
        this.host = host;
        this.port = port;
        this.topic = topic;
        this.stream = stream;
        // TODO set member variables
        
    }

    @Override
    public void close() {
        super.close();
        kafkaConsumerThread.close();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        final Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("bootstrap.servers", host + ":" + port);
        consumer = new KafkaConsumer<>(props);
        System.out.println(consumer);
        this.collector = collector;
        tweetsToEmit = new ArrayList<>();
        // TODO Configure, create, and subscribe user to topics
        consumer.subscribe(Arrays.asList(topic));
        kafkaConsumerThread = new KafkaConsumerThread();
        new Thread(kafkaConsumerThread).run();
    }

    @Override
    public void nextTuple() {
        // TODO Poll consumer for records, outputting to the correct topic
        // and a tuple of (key, value) of the record
        System.out.println("next tuple called");
        lock();
        if (!tweetsToEmit.isEmpty()) {
            System.out.println("adding element");
            collector.emit(stream, new Values(topic, tweetsToEmit.remove(0)));
        }
        lock = false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO declare output stream for each configured stream
        declarer.declareStream("spout_tweets", new Fields("topic", "value"));
    }

    private void lock() {
        while (lock) {
            try {
                Thread.sleep(10);
                System.out.println("locked");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        lock = true;
    }

    class KafkaConsumerThread implements Runnable {

        private boolean consume = true;

        @Override
        public void run() {
            BinaryDecoder decode = null;
            SpecificDatumReader<Tweet> reading = new SpecificDatumReader<>(Tweet.getClassSchema());
            Tweet tweet = null;
            System.out.println("consuming");
            while (consume) {
                ConsumerRecords<String, byte[]> records = consumer.poll(10);

                lock();
                for (ConsumerRecord<String, byte[]> record : records) {
                    tweetsToEmit.add(record.value());
                }
                lock = false;
                //                System.out.println("consumed");
            }
            consumer.close();
        }

        public void close() {
            consume = false;
        }
    }
}