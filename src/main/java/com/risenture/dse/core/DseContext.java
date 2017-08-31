package com.risenture.dse.core;

import java.io.Closeable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.risenture.dse.graph.GraphMappingManager;

public class DseContext implements Closeable {
	private final DseCluster cluster;
	private final DseSession session;
	private final GraphMappingManager mappingManager;

	/**
	 * Initializes a datastax enterprise context with the passed initializer implementation Creates
	 * {@link Cluster}, {@link Session}, {@link GraphMappingManager} instances. The session object that is
	 * created is not tied to any keyspace.this requires all queries to have table names to be
	 * prefixed by keyspace
	 *
	 * @param cluster
	 */
	public DseContext(DseCluster cluster) {
		this.cluster = cluster;
		// creates a session not tied to any keyspace. this requires all queries to be prefixed by
		// keyspace
		session = cluster.connect();
		mappingManager = new GraphMappingManager(this.session);
	}

	public DseCluster getCluster() {
		return cluster;
	}

	public DseSession getSession() {
		return session;
	}

	public GraphMappingManager getMappingManager() {
		return mappingManager;
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
