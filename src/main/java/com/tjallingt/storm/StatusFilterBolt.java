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
import java.util.Set;
import java.util.stream.Collectors;

public class StatusFilterBolt extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatusFilterBolt.class);
	private final String host = "localhost";
	private transient JedisPool pool;
	private boolean filterRetweets;
	private boolean filterSensitive;
	private Set<String> whitelistedLanguages;

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
		ArrayList<String> filters = new ArrayList<>();

		if(isWhitelistedLanguage(status) == false) {
			filters.add("status:languages");
		}

		if (filterRetweets && status.isRetweet()) {
			filters.add("status:retweets");
		}

		if (filterSensitive && status.isPossiblySensitive()) {
			filters.add("status:sensitive");
		}

		collector.emit(new Values(status.getId(), status, json, filters));
	}

	public boolean isWhitelistedLanguage(Status status) {
		// if there is nothing everything is whitelisted
		if (whitelistedLanguages.size() == 0) return true;
		return whitelistedLanguages.contains(status.getLang());
	}

	public void updateCache() {
		try (Jedis jedis = getPoolResource()) {
			filterRetweets = jedis.getbit("settings:status:retweets", 0);
			filterSensitive = jedis.getbit("settings:status:sensitive", 0);
			whitelistedLanguages = jedis.smembers("settings:status:languages").stream().collect(Collectors.toSet());
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
