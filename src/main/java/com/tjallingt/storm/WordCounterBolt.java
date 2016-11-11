package com.tjallingt.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Keeps stats on word count, calculates and logs top words every X second to stdout and top list every Y seconds,
 * @author davidk
 */
public class WordCounterBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
	/** Number of seconds before the top list will be logged to stdout. */
	private final long logIntervalSec;
	/** Number of seconds before the top list will be cleared. */
	private final long clearIntervalSec;
	/** Number of top words to store in stats. */
	private final int topListSize;

	private final String channel = "wordCount";

	private final String host = "localhost";
	private transient JedisPool pool;


	private Map<String, Long> counter;
	private long lastLogTime;
	private long lastClearTime;
	private OutputCollector collector;

	public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
		this.logIntervalSec = logIntervalSec;
		this.clearIntervalSec = clearIntervalSec;
		this.topListSize = topListSize;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		counter = new HashMap<String, Long>();
		lastLogTime = System.currentTimeMillis();
		lastClearTime = System.currentTimeMillis();
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = counter.get(word);
		count = count == null ? 1L : count + 1;
		counter.put(word, count);

		//logger.info(word + '>' + String.valueOf(count));

		long now = System.currentTimeMillis();
		long logPeriodSec = (now - lastLogTime) / 1000;
		if (logPeriodSec > logIntervalSec) {
			logger.info("Word count: "+counter.size());
			publishTopList();
			lastLogTime = now;
		}
	}

	private void publishTopList() {
		// calculate top list:
		SortedMap<Long, String> top = new TreeMap<Long, String>();
		for (Map.Entry<String, Long> entry : counter.entrySet()) {
			// enter value (count) and key (word) in a sorted map as key-value respectively
			top.put(entry.getValue(), entry.getKey());
			if (top.size() > topListSize) {
				// remove first item from SortedMap (lowest value)
				top.remove(top.firstKey());
			}
		}

		// Output top list:
		for (Map.Entry<Long, String> entry : top.entrySet()) {
			// emit and log top values from sorted list
			collector.emit(new Values(entry.getValue(), entry.getKey()));
			logger.info("top - " + entry.getValue() + '>' + entry.getKey().toString());
		}

		try (Jedis jedis = getPoolResource()){
			jedis.publish(channel, "update");
		}

		// Clear top list
		long now = System.currentTimeMillis();
		if (now - lastClearTime > clearIntervalSec * 1000) {
			counter.clear();
			lastClearTime = now;
		}
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
