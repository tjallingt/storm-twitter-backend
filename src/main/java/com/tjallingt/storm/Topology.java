package com.tjallingt.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Topology class that sets up the Storm topology for this sample.
 * Please note that Twitter credentials have to be provided as VM args, otherwise you'll get an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 * Adapted from storm-twitter-word-count by davidkiss
 */
public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-backend";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		// http://stackoverflow.com/questions/31851311/how-to-send-output-of-two-different-spout-to-the-same-bolt
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterFilterSpout", new TwitterFilterSpout());
		b.setSpout("RedisSubSpout", new RedisSubSpout("update-cache"));
		b.setBolt("BlacklistBolt", new BlacklistBolt()).shuffleGrouping("TwitterFilterSpout").shuffleGrouping("RedisSubSpout");
		b.setBolt("RedisStoreBolt", new TweetStoreBolt()).shuffleGrouping("BlacklistBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});


	}

}
