package com.tjallingt.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.Map;

public class StatusFilterBolt extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusFilterBolt.class);
	private final String host = "localhost";
	private transient JedisPool pool;
	private boolean filterRetweet;
	private boolean filterSensitive;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		updateCache();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// !! WARNING !! WARNING !! WARNING !! WARNING !!
		// !! WARNING !! WARNING !! WARNING !! WARNING !!
		// this breaks when someone renames the RedisSpout
		// really fragile...

		if (tuple.getSourceComponent().equals("RedisSpout")) {
			if (tuple.getStringByField("channel").equals("update-cache")) {
				updateCache();
			}
			return;
		}

		Status status = (Status) tuple.getValueByField("status");
		String json = tuple.getStringByField("json");
		ArrayList<String> filters = (ArrayList<String>) tuple.getValueByField("filters");

		if (filterRetweet && status.isRetweet()) {
			filters.add("filter:retweet");
		}

		if (filterSensitive && status.isPossiblySensitive()) {
			filters.add("filter:sensitive");
		}

		collector.emit(new Values(status.getId(), status, json, filters));
	}

	public void updateCache() {
		try (Jedis jedis = getPoolResource()) {
			filterRetweet = jedis.getbit("settings:filter:retweet", 0);
			filterSensitive = jedis.getbit("settings:filter:sensitive", 0);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "status", "json", "filters"));
	}

	private Jedis getPoolResource() {
		if (pool == null) {
			pool = new JedisPool(new JedisPoolConfig(), host);
		}
		return pool.getResource();
	}

	@Override
	public void cleanup() {
		pool.destroy();
	}
}
