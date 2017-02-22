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
import java.util.stream.LongStream;

public class BlacklistBolt extends BaseBasicBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BlacklistBolt.class);
	private final String host = "localhost";
	private transient JedisPool pool;
	private Set<String> wordBlacklist;
	private long[] userBlacklist;

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

		Status checkStatus = status;

		// TODO: should check regular status?
		if (checkStatus.isRetweet()) {
			checkStatus = checkStatus.getRetweetedStatus();
		}

		if (isUserBlacklisted(checkStatus)) {
			filters.add("blacklist:user");
		}
		if (isWordBlacklisted(checkStatus)) {
			filters.add("blacklist:word");
		}

		collector.emit(new Values(status.getId(), status, json, filters));
	}

	public boolean isUserBlacklisted(Status status) {
		long userId = status.getUser().getId();
		return LongStream.of(userBlacklist).anyMatch(id -> id == userId);
	}

	public boolean isWordBlacklisted(Status status) {
		// TODO: use extended_tweet.full_text if available
		String text = status.getText().toLowerCase();
		return wordBlacklist.stream().anyMatch(text::contains);
	}

	public void updateCache() {
		try (Jedis jedis = getPoolResource()) {
			wordBlacklist = jedis.smembers("settings:blacklist:words").stream().map(String::toLowerCase).collect(Collectors.toSet());
			userBlacklist = jedis.smembers("settings:blacklist:users").stream().mapToLong(Long::parseLong).toArray();
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
