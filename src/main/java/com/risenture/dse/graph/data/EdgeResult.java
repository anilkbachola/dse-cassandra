package com.risenture.dse.graph.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.risenture.dse.graph.EdgeMapper;
import com.risenture.dse.graph.PropertyMapper;

public class EdgeResult<T,S,E>  extends NodeResult<T>{

  private final GraphResultSet grs;
  private final EdgeMapper<T,S,E> edgeMapper;

  public EdgeResult(GraphResultSet grs, EdgeMapper<T,S,E> edgeMapper) {
    super();
    this.grs = grs;
    this.edgeMapper = edgeMapper;
  }

  private T map(GraphNode node) {
    T vertex = edgeMapper.newEntity();
    for (PropertyMapper<T> pm : edgeMapper.allProperties()) {
      Object value = node.asEdge().getProperty(pm.getPropertyName()).getValue();

      if (shouldSetValue(value)) {
        pm.setValue(vertex, value);
      }
    }
    PropertyMapper<T> fromPm = edgeMapper.getFromProperty();
    PropertyMapper<T> toPm = edgeMapper.getToProperty();

    return vertex;
  }

  private static boolean shouldSetValue(Object value) {
    if (value == null)
      return false;
    if (value instanceof Collection)
      return !((Collection<?>) value).isEmpty();
    if (value instanceof Map)
      return !((Map<?,?>) value).isEmpty();
    return true;
  }

  @Override
  public T one() {
    GraphNode node = grs.one();
    return node == null ? null : map(node);
  }

  @Override
  public List<T> all() {
    List<GraphNode> nodes = grs.all();
    List<T> vertexes = new ArrayList<T>(nodes.size());
    for (GraphNode node : nodes) {
      vertexes.add(map(node));
    }
    return vertexes;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private final Iterator<GraphNode> nodeIterator = grs.iterator();

      @Override
      public boolean hasNext() {
        return nodeIterator.hasNext();
      }

      @Override
      public T next() {
        return map(nodeIterator.next());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return grs.getExecutionInfo();
  }
}
