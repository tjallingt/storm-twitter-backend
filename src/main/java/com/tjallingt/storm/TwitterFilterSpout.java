package com.tjallingt.storm;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterFilterSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Values> queue;
	private TwitterStream twitterStream;
	private final String host = "localhost";
	private transient JedisPool pool;

	public class Listener implements Runnable {
		private transient JedisPool pool;

		public Listener(JedisPool pool) {
			this.pool = pool;
		}

		public void run() {
			JedisPubSub listener = new JedisPubSub() {
				public void onMessage(String channel, String message) {
					// TODO: prevent update filter from being called several times in a row
					updateFilter();
				}
			};

			try (Jedis jedis = pool.getResource()) {
				// TODO: add support for psubscribe?
				jedis.subscribe(listener, "update-filter");
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Values>(1000);
		this.collector = collector;

		StatusListener twitterListener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(new Values(status, TwitterObjectFactory.getRawJSON(status)));
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("onDeletionNotice: " + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("onTrackLimitationNotice: " + Integer.toString(numberOfLimitedStatuses));
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("onScrubGeo");
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
				System.out.println("onStallWarning: " + stallWarning.getMessage());
			}

			@Override
			public void onException(Exception e) {
				System.out.println("onException: " + e.getMessage());
			}
		};

		TwitterStreamFactory factory = new TwitterStreamFactory();
		twitterStream = factory.getInstance();
		twitterStream.addListener(twitterListener);

		updateFilter();

		Thread listener = new Thread(new Listener(pool));
		listener.start();
	}

	@Override
	public void nextTuple() {
		Values ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(ret);
		}
	}

	public void updateFilter() {
		FilterQuery filter = new FilterQuery();
		try (Jedis jedis = getPoolResource()) {
			String[] track = jedis.smembers("settings:filter:track").stream().toArray(String[]::new);
			long[] follow = jedis.smembers("settings:filter:follow").stream().mapToLong(Long::parseLong).toArray();
			filter.track(track);
			filter.follow(follow);
			// TODO: add language/location
		}
		twitterStream.filter(filter);
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
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

}
