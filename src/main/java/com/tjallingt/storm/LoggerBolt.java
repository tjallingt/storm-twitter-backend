package com.tjallingt.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Stores words and counts in a Redis hash
 * Created by Tjalling on 11-3-2016.
 */
class LoggerBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {}

	@Override
	public void execute(Tuple input) {
		String channel = (String) input.getValueByField("channel");
		String message = (String) input.getValueByField("message");

		logger.info("Channel: " + channel + " - Message: " + message);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}