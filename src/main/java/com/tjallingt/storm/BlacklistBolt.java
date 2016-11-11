package com.tjallingt.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import twitter4j.Status;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class BlacklistBolt extends BaseBasicBolt {
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
		if (tuple.getSourceComponent().equals("RedisSubSpout")) {
			if (tuple.getStringByField("channel").equals("update-cache")) {
				updateCache();
			}
			return;
		}

		Status status = (Status) tuple.getValueByField("status");
		// TODO: case insensitive username?
		if (!isBlacklisted(status)) {
			collector.emit(tuple.getValues());
		}
	}

	public boolean isBlacklisted(Status status) {
		// check if user is blacklisted
		long userId = status.getUser().getId();
		boolean isUserBlacklisted = LongStream.of(userBlacklist).anyMatch(id -> id == userId);
		if (isUserBlacklisted) {
			return true;
		}
		// if the tweet is a retweet check if the original tweet is blacklisted
		// if the status is a retweet we only check the text in the original, maybe this isn't ideal?
		if (status.isRetweet()) {
			return isBlacklisted(status.getRetweetedStatus());
		}
		// check if a word in the text is blacklisted
		String text = status.getText().toLowerCase();
		boolean isWordBlacklisted = wordBlacklist.stream().anyMatch(text::contains);
		if (isWordBlacklisted) {
			return true;
		}
		return false;
	}

	public void updateCache() {
		try (Jedis jedis = getPoolResource()) {
			wordBlacklist = jedis.smembers("settings:blacklist:words").stream().map(String::toLowerCase).collect(Collectors.toSet());
			userBlacklist = jedis.smembers("settings:blacklist:users").stream().mapToLong(Long::parseLong).toArray();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status", "json"));
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
