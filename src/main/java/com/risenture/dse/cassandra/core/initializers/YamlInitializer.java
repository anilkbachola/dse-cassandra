package com.risenture.dse.cassandra.core.initializers;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Initializer;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host.StateListener;
import com.datastax.driver.core.ProtocolOptions;
import com.risenture.dse.cassandra.core.ClusterStateListener;

import com.risenture.dse.exception.InitializationException;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class YamlInitializer implements Initializer {

  public static final String DEAULT_CONFIG = "/cassandra.yml";
  private final CassandraConfig config;

  /**
   * Creates an instance of {@link Initializer} from cassandra.yml file in classpath.
   */
  public YamlInitializer() {
    Yaml yaml = new Yaml();
    InputStream in = YamlInitializer.class.getResourceAsStream(DEAULT_CONFIG);
    if(in == null) {
      throw new InitializationException("cassandra.yml file could not be found in classpath");
    }
    config = yaml.loadAs( in, CassandraConfig.class );
  }

  /**
   * Creates an instance of {@link Initializer} from passed yaml file name in classpath.
   * @param configFile name of config file
   */
  public YamlInitializer(String configFile) {
    Yaml yaml = new Yaml();
    InputStream in = YamlInitializer.class.getResourceAsStream(configFile);
    if(in == null) {
      throw new InitializationException("cassandra.yml file could not be found in classpath");
    }
    config = yaml.loadAs( in, CassandraConfig.class );
  }

  @Override
  public String getClusterName() {
    return config.getClusterName();
  }

  @Override
  public List<InetSocketAddress> getContactPoints() {
    List<InetSocketAddress> contactaddress = new ArrayList<InetSocketAddress>();
    List<String> contactpoints = config.getContactpoints();
    //int configPort = config.getConfiguration().getProtocolOptions().getPort();
    int port = ProtocolOptions.DEFAULT_PORT;
    if (config.getProtocolOptions() != null
        && config.getProtocolOptions().getPort() > 0) {
      port = config.getProtocolOptions().getPort();
    }
    for (String contactpoint: contactpoints) {
      contactaddress.add(new InetSocketAddress(contactpoint, port));
    }
    return contactaddress;
  }

  @Override
  public Configuration getConfiguration() {
    Configuration.Builder builder = Configuration.builder();
    if (config.getProtocolOptions() != null) {
      builder.withProtocolOptions(config.getProtocolOptions());
    }
    return Cluster.builder().getConfiguration();
  }

  @Override
  public Collection<StateListener> getInitialListeners() {
    List<StateListener> listeners = new ArrayList<>();
    listeners.add(new ClusterStateListener());
    return listeners;
  }

}
