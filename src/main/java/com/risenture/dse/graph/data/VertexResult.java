package com.risenture.dse.graph.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.risenture.dse.graph.PropertyMapper;
import com.risenture.dse.graph.VertexMapper;

public class VertexResult<T> extends NodeResult<T>{

	private final GraphResultSet grs;
	private final VertexMapper<T> vertexMapper;

	public VertexResult(GraphResultSet grs, VertexMapper<T> vertexMapper) {
		super();
		this.grs = grs;
		this.vertexMapper = vertexMapper;
	}

	private T map(GraphNode node) {
		T vertex = vertexMapper.newEntity();
		for (PropertyMapper<T> pm : vertexMapper.allProperties()) {
			Object value = node.asVertex().getProperty(pm.getPropertyName()).getValue().asString();

			if (shouldSetValue(value)) {
				pm.setValue(vertex, value);
			}
		}
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
	public List<T> all() {
		List<GraphNode> nodes = grs.all();
		List<T> vertexes = new ArrayList<T>(nodes.size());
		for (GraphNode node : nodes) {
			vertexes.add(map(node));
		}
		return vertexes;
	}

	@Override
	public T one() {
		GraphNode node = grs.one();
		return node == null ? null : map(node);
	}

	@Override
	public ExecutionInfo getExecutionInfo() {
		 return grs.getExecutionInfo();
	}

	
}
