package com.risenture.dse.graph.data;

import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;

import java.util.Map;

public class DseGraphTemplate {

  private final DseSession session;

  public DseGraphTemplate(DseSession session) {
    this.session = session;
  }

  public DseSession getSession() {
    return session;
  }

  public GraphResultSet executeGraph(String query) {
    return getSession().executeGraph(query);
  }

  public GraphResultSet executeGraph(String query, Map<String, Object> values) {
    return getSession().executeGraph(query, values);
  }

  public GraphResultSet executeGraph(GraphStatement statement) {
    return getSession().executeGraph(statement);
  }

  public ListenableFuture<GraphResultSet> executeGraphAsync(String query) {
    return getSession().executeGraphAsync(query);
  }

  public ListenableFuture<GraphResultSet> executeGraphAsync(String query, 
      Map<String, Object> values) {
    return getSession().executeGraphAsync(query, values);
  }

  public ListenableFuture<GraphResultSet> executeGraphAsync(GraphStatement statement) {
    return getSession().executeGraphAsync(statement);
  }

}
