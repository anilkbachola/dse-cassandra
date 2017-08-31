package com.risenture.dse.cassandra.core.initializers;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Initializer;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host.StateListener;
import com.datastax.driver.core.ProtocolOptions;

import com.risenture.dse.cassandra.core.ClusterStateListener;
import com.risenture.dse.exception.InitializationException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DefaultInitializer implements Initializer {

  private final String clusterName;
  private final String contactPoints;
  private final int port;

  /**
   *
   * @param contactPoints
   * @param clusterName
   */
  public DefaultInitializer(String contactPoints, String clusterName) {
    if(contactPoints == null || clusterName == null) {
      throw new InitializationException("ContactPoints and ClusterName are mandatory to initialize");
    }
    this.clusterName = clusterName;
    this.contactPoints = contactPoints;
    this.port = ProtocolOptions.DEFAULT_PORT;
  }

  public DefaultInitializer(String contactPoints, String clusterName, int port) {
    this.clusterName = clusterName;
    this.contactPoints = contactPoints;
    this.port = port;
  }

  @Override
  public String getClusterName() {
    return this.clusterName;
  }

  @Override
  public List<InetSocketAddress> getContactPoints() {
    String[] contactpoints = contactPoints.split(",") ;
    List<InetSocketAddress> contactaddress = new ArrayList<InetSocketAddress>();
    for (String contactpoint : contactpoints) {
      contactaddress.add(new InetSocketAddress(contactpoint, port));
    }
    return contactaddress;
  }

  //TODO: implement this
  @Override
  public Configuration getConfiguration() {
    return new Cluster.Builder().getConfiguration();
  }

  @Override
  public List<StateListener> getInitialListeners() {
    List<StateListener> listeners = new ArrayList<>();
    listeners.add(new ClusterStateListener());
    return listeners;
  }
}
