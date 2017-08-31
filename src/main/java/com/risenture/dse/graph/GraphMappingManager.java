package com.risenture.dse.graph;

import com.risenture.dse.core.GraphAnnotationParser;

import com.datastax.driver.dse.DseSession;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GraphMappingManager {

  private volatile Map<Class<?>, GraphMapper<?>> mappers = Collections.emptyMap();
  private final DseSession session;

  public GraphMappingManager(DseSession session) {
    this.session = session;
  }

  public DseSession session() {
    return this.session;
  }

  /**
   * return mapping manager for the class.
   * @param klass class
   * @return mapper
   */
  public <T> GraphMapper<T> mapper(Class<T> klass) {
    return getMapper(klass);
  }

  @SuppressWarnings("unchecked")
  private <T> GraphMapper<T> getMapper(Class<T> klass) {
    GraphMapper<T> graphMapper = (GraphMapper<T>) mappers.get(klass);
    if (graphMapper == null) { // build the mapper for this class
      synchronized (mappers) {
        graphMapper = (GraphMapper<T>) mappers.get(klass);
        if (graphMapper == null) { // if mapper not found, create new mapper by parsing

          NodeMapper<T> nodeMapper = GraphAnnotationParser.parseGraphNodes(klass);
          graphMapper = new GraphMapper<>(this, nodeMapper, klass);

          Map<Class<?>, GraphMapper<?>> newMappers = new HashMap<Class<?>, GraphMapper<?>>(mappers);
          newMappers.put(klass, graphMapper);
          mappers = newMappers;
        }
      }
    }
    return graphMapper;
  }
}
