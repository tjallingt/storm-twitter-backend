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

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings({ "rawtypes", "serial" })
public class RedisSubSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Values> queue;
	private final String host = "localhost";
	private String[] channels;
	private transient JedisPool pool;

	public RedisSubSpout(String... channels) {
		this.channels = channels;
	}

	public class Listener implements Runnable {
		private transient JedisPool pool;
		private String[] channels;
		private LinkedBlockingQueue<Values> queue;

		public Listener(JedisPool pool, String[] channels, LinkedBlockingQueue<Values> queue) {
			this.pool = pool;
			this.channels = channels;
			this.queue = queue;
		}

		public void run() {
			JedisPubSub listener = new JedisPubSub() {
				public void onMessage(String channel, String message) {
					queue.offer(new Values(channel, message));
				}

				public void onSubscribe(String channel, int subscribedChannels) {
				}

				public void onUnsubscribe(String channel, int subscribedChannels) {
				}

				public void onPSubscribe(String pattern, int subscribedChannels) {
				}

				public void onPUnsubscribe(String pattern, int subscribedChannels) {
				}

				public void onPMessage(String pattern, String channel, String message) {
				}
			};

			try (Jedis jedis = pool.getResource()) {
				// TODO: add support for psubscribe?
				jedis.subscribe(listener, channels);
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		queue = new LinkedBlockingQueue<Values>(1000);
		pool = new JedisPool(new JedisPoolConfig(), host);
		Thread listener = new Thread(new Listener(pool, channels, queue));
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

	@Override
	public void close() {
		pool.destroy();
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
		declarer.declare(new Fields("channel", "message"));
	}
}
