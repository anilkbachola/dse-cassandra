package com.risenture.dse.cassandra.core;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Initializer;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.risenture.dse.exception.InitializationException;

import java.io.Closeable;

public class CassandraContext implements Closeable {
  private final Cluster cluster;
  private final Session session;
  private final MappingManager mappingManager;

  /**
   * Initializes a cassandra context with the passed initializer implementation.
   * Creates {@link Cluster}, {@link Session}, {@link MappingManager} instances.
   * The session object created is not tied to any keyspace,
   * this requires table names to be prefixed by keyspace in all queries
   *
   * <p>Make sure there is only one instance of this class for a single application
   * @param initializer A custom implementation of {@link Initializer} or
   *     an instance of default implementation {@link Cluster.Builder}
   */
  public CassandraContext(Initializer initializer) {
    if(initializer == null) {
      throw new InitializationException("Failed initialize Cassandra Context. Initializer is null");
    }
    this.cluster = Cluster.buildFrom(initializer);
    // creates a session not tied to any keyspace.
    session = cluster.connect();
    mappingManager = new MappingManager(this.session);
  }

  /**
   * Initializes a cassandra context with the passed initializer implementation.
   * Creates {@link Cluster}, {@link Session}, {@link MappingManager} instances.
   *
   * <p>Make sure there is only one instance of this class for a single application
   * @param initializer A custom implementation of {@link Initializer} or
   *     an instance of default implementation {@link Cluster.Builder}
   */
  public CassandraContext(String keySpace, Initializer initializer) {
    if(initializer == null || keySpace == null) {
      throw new InitializationException("Failed initialize Cassandra Context. Initializer is null or keyspace is null");
    }
    this.cluster = Cluster.buildFrom(initializer);
    session = cluster.connect(keySpace);
    mappingManager = new MappingManager(this.session);
  }

  public Cluster getCluster() {
    return cluster;
  }

  public Session getSession() {
    return session;
  }

  public MappingManager getMappingManager() {
    return mappingManager;
  }

  /**
   * Return the implementation of the accessor.
   *
   * @param klass entity class
   * @return
   * @return accessor
   */
  public Object createAccessor(Class<?> klass) {
    return mappingManager.createAccessor(klass);
  }

  @Override
  public void close() {
    if (!session.isClosed()) {
      session.close();
    }

    if (!cluster.isClosed()) {
      cluster.close();
    }
  }

}
