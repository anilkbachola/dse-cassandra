package com.risenture.dse.cassandra.core.initializers;

import com.datastax.driver.core.ProtocolOptions;

import java.util.List;

public class CassandraConfig {

  private List<String> contactpoints;
  private String clusterName;
  private ProtocolOptions protocolOptions;

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public List<String> getContactpoints() {
    return contactpoints;
  }

  public void setContactpoints(List<String> contactpoints) {
    this.contactpoints = contactpoints;
  }

  public ProtocolOptions getProtocolOptions() {
    return protocolOptions;
  }

  public void setProtocolOptions(ProtocolOptions protocolOptions) {
    this.protocolOptions = protocolOptions;
  }
}
