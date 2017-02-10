package com.tjallingt.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;

class TweetStoreBolt extends BaseBasicBolt {
	private final String host = "localhost";
	private transient JedisPool pool;
	private int storedTweets = 0;
	private static final Logger logger = LoggerFactory.getLogger(TweetStoreBolt.class);

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String json = tuple.getStringByField("json");
		ArrayList<String> filters = (ArrayList<String>) tuple.getValueByField("filters");
		String listKey;
		int listSize;
		if (filters.size() > 0) {
			listKey = "filterList";
			listSize = 10;

			json = json.substring(0, json.length() - 1);
			json += ",\"filters\": [\"" + String.join("\",\"", filters) + "\"]}";
		} else {
			listKey = "tweetList";
			listSize = 50;
		}

		//logger.info("Storing tweet: " + Integer.toString(++storedTweets) + " in " + listKey);
		try (Jedis jedis = getPoolResource()){
			// make sure to execute adding and trimming at the same time
			Transaction transaction = jedis.multi();
			transaction.lpush(listKey, json);
			transaction.ltrim(listKey, 0, listSize - 1);
			transaction.exec();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

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