package com.risenture.dse.cassandra.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Host.StateListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStateListener implements StateListener {

  private static final Logger logger = LoggerFactory.getLogger(ClusterStateListener.class);

  @Override
  public void onAdd(Host host) {
    logger.info("Node with address [{}] added to the cluster",host.getAddress());
  }

  @Override
  public void onUp(Host host) {
    logger.info("Node with address [{}] is up",host.getAddress());
  }

  @Override
  public void onDown(Host host) {
    logger.info("Node with address [{}] is down",host.getAddress());
  }

  @Override
  public void onRemove(Host host) {
    logger.info("Node with address [{}] removed from the cluster",host.getAddress());
  }

  @Override
  public void onRegister(Cluster cluster) {
    logger.info("state change listener is registered with the cluster [{}]",
        cluster.getClusterName());
  }

  @Override
  public void onUnregister(Cluster cluster) {
    logger.info("state change listener is unregistered with the cluster [{}]",
        cluster.getClusterName());
  }

}
