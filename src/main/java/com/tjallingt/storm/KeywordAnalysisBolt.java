package com.tjallingt.storm;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.base.Splitter;
import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import org.apache.storm.task.TopologyContext;
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
import twitter4j.Status;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class KeywordAnalysisBolt extends BaseBasicBolt {
	private final String host = "localhost";
	private transient JedisPool pool;
	private Cache<String, Integer> keywords;
	private static final Splitter SPLITTER = Splitter.on(CharMatcher.whitespace())
			.trimResults(CharMatcher.anyOf("[]()<>.,!:;*"))
			.omitEmptyStrings();

	private static final Logger logger = LoggerFactory.getLogger(KeywordAnalysisBolt.class);

	public void prepare(Map stormConf, TopologyContext context) {
		

		keywords = CacheBuilder.newBuilder()
			.maximumSize(500)
			.expireAfterWrite(30, TimeUnit.SECONDS)
			.removalListener((RemovalListener<String, Integer>) notification -> {
				// do stuff?
			})
			.build();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Status status = (Status) tuple.getValueByField("status");
		ArrayList<String> filters = (ArrayList<String>) tuple.getValueByField("filters");
		// only if there are no filters affecting this status
		if (filters.size() == 0) {
			// TODO: use extended_tweet.full_text if available
			String text = status.getText().toLowerCase();
			List<String> textList = Lists.newArrayList(SPLITTER.split(text));
			Map<String, Integer> words = new ArrayList<>(textList)
					.stream()
					.filter(word -> word.length() > 3)
					.filter(word -> word.startsWith("#") || word.startsWith("@"))
					.distinct()
					// TODO: check collection settings
					.filter(word -> !word.contains("pinkpop"))
					.filter(word -> !word.contains("pp15"))
					.collect(Collectors.toMap(word -> word, word -> {
						Integer count = keywords.getIfPresent(word);
						if (count == null) count = 0;
						return count + 1;
					}));
			keywords.putAll(words);
		}

		Map<String, Integer> words = keywords.asMap();
		if (words.size() > 0) {
			String max = words.entrySet().stream()
					.max(Comparator.comparingInt(Map.Entry::getValue))
					.map(Map.Entry::getKey)
					.orElse("");

			try (Jedis jedis = getPoolResource()){
				// TODO: finetune
				if (words.get(max) > 4) {
					logger.info("Found suggestion keyword: " + max);
					jedis.sadd("data:analysis:keywords", max);
					keywords.invalidate(max);
				}
			}
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