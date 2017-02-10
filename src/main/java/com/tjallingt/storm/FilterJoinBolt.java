package com.tjallingt.storm;

import com.google.common.cache.*;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class FilterJoinBolt extends BaseRichBolt {
	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterJoinBolt.class);
	OutputCollector collector;
	int numSources;
	Cache<Long, Map<GlobalStreamId, Tuple>> pending;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
		pending = CacheBuilder.newBuilder()
				.maximumSize(100)
				.expireAfterWrite(timeout, TimeUnit.SECONDS)
				.removalListener((RemovalListener<Long, Map<GlobalStreamId, Tuple>>) notification -> {
					Map<GlobalStreamId, Tuple> tuples = notification.getValue();
					for (Tuple tuple : tuples.values()) {
						collector.fail(tuple);
					}
				})
				.build();

		numSources = context.getThisSources().size();
	}

	@Override
	public void execute(Tuple tuple) {
		Long id = tuple.getLongByField("id");
		Status status = (Status) tuple.getValueByField("status");
		String json = tuple.getStringByField("json");

		GlobalStreamId streamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());

		Map<GlobalStreamId, Tuple> parts = pending.getIfPresent(id);
		if (parts == null) {
			parts = new HashMap<>();
		}

		if (parts.containsKey(streamId)) {
			throw new RuntimeException("Received same side of single join twice");
		}

		parts.put(streamId, tuple);

		if (parts.size() < numSources) {
			pending.put(id, parts);
		} else {
			pending.invalidate(id);

			ArrayList<String> filters = new ArrayList<>();
			for (Tuple part : parts.values()) {
				filters.addAll((ArrayList<String>) part.getValueByField("filters"));
			}

			collector.emit(new Values(status, json, filters));

			for (Tuple part : parts.values()) {
				collector.ack(part);
			}
		}
	}



	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status", "json", "filters"));
	}

	@Override
	public void cleanup() {
	}
}
