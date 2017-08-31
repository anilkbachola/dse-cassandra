package com.risenture.dse.cassandra.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryLatencyTracker implements LatencyTracker {

  private static final Logger logger = LoggerFactory.getLogger(QueryLatencyTracker.class);

  @Override
  public void update(Host host, Statement statement, Exception exception,
      long newLatencyNanos) {
    logger.info("Statement [{}] on Node [{}] executed with latency [{}] nanos, with error {}",
        statement, host.getAddress(), newLatencyNanos, exception);
  }

  @Override
  public void onRegister(Cluster cluster) {
    logger.info("Query latency tracker registered with the cluster [{}]", cluster.getClusterName());
  }

  @Override
  public void onUnregister(Cluster cluster) {
    logger.info("Query latency tracker unregistered with the cluster [{}]", 
        cluster.getClusterName());
  }

}
