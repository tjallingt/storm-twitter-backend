package com.tjallingt.storm;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import twitter4j.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterLineSpout extends BaseRichSpout {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TwitterLineSpout.class);
	private SpoutOutputCollector collector;
	private String filename;
	private int maxsleep;
	private BufferedReader reader;

	public TwitterLineSpout(String filename, int maxsleep) {
		this.filename = filename;
		this.maxsleep = maxsleep;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			reader = new BufferedReader(new FileReader(filename));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void nextTuple() {
		try {
			String json = reader.readLine();

			if (json != null) {
				Status status = TwitterObjectFactory.createStatus(json);
 				collector.emit(new Values(status.getId(), status, json));
				Thread.sleep((long)(Math.random() * maxsleep)); // randomly sleep for up to 1 second
			} else {
				logger.info("reached apparent end of file, reloading file");
				reader = new BufferedReader(new FileReader(filename));
			}

		} catch (Exception e) { // just error out on any exception...
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			logger.info("Error closing file handle");
		}
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
		declarer.declare(new Fields("id", "status", "json"));
	}

}
