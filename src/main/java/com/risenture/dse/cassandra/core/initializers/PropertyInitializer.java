package com.risenture.dse.cassandra.core.initializers;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.datastax.driver.core.Cluster.Initializer;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host.StateListener;
import com.risenture.dse.cassandra.core.ClusterStateListener;

//TODO: Implement this class
public class PropertyInitializer implements Initializer{

  public static final String DEAULT_CONFIG = "cassandra.properties";
  private final CassandraConfig config = null;

  public PropertyInitializer(){

  }

  public PropertyInitializer(String configFile, String contactPoints, String clusterName){

  }

  @Override
  public String getClusterName() {
    return config.getClusterName();
  }

  @Override
  public List<InetSocketAddress> getContactPoints() {

    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Collection<StateListener> getInitialListeners() {
    List<StateListener> listeners = new ArrayList<>();
    listeners.add(new ClusterStateListener());
    return listeners;
  }

}
