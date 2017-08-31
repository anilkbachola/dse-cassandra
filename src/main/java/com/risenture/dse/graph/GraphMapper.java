package com.risenture.dse.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;
import com.datastax.driver.dse.graph.SimpleGraphStatement;
import com.risenture.dse.graph.data.EdgeResult;
import com.risenture.dse.graph.data.NodeResult;
import com.risenture.dse.graph.data.VertexResult;

public class GraphMapper<T> {

	private final GraphMappingManager mappingManager;
	private final NodeMapper<T> nodeMapper;
	private final Class<T> klass;
	
	private final String DOT = ".";
	private final String COMMA = ",";
	private final String LEFT_PARENTHESIS = "(";
	private final String RIGHT_PARENTHESIS = ")";
	private final String PROPERTY = "property";
	private final String HAS = "has";
	
	//cache the GraphStatements
	ConcurrentHashMap<QueryKey, GraphStatement> statementCache = new ConcurrentHashMap<>();
	
	public GraphMapper(GraphMappingManager mappingManager,
			NodeMapper<T> nodeMapper, Class<T> klass) {
		super();
		this.mappingManager = mappingManager;
		this.nodeMapper = nodeMapper;
		this.klass = klass;
	}
	
	/**
	 * at the time of writing Dse Graph only supports gremlin traversal apis addV() and addE() for insertion
	 * @param <E>
	 * @param <S>
	 * @param node
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <E, S> GraphStatement createQuery(T node){
		Map<PropertyMapper<T>, Object> propValMap = new HashMap<>();
		for(PropertyMapper<T> pm: nodeMapper.allProperties()){
			Object value = pm.getValue(node);
			//TODO: add a config param to decide whether to consider null values. to ignore properties if their value is null
			propValMap.put(pm, value);
		}
		//if node is a vertex
		if(nodeMapper instanceof VertexMapper<?>){
			StringBuffer sb = new StringBuffer();
			sb.append("g.addV(label,vertexLabel)");
			for(Map.Entry<PropertyMapper<T>, Object> entry: propValMap.entrySet()){
				sb.append(DOT).append(PROPERTY)
				.append(LEFT_PARENTHESIS)
				.append(quote(entry.getKey().getPropertyName()))
				.append(COMMA)
				.append(quote(entry.getValue().toString()))
				.append(RIGHT_PARENTHESIS);
			}
			
			SimpleGraphStatement sgs = new SimpleGraphStatement(sb.toString());
			sgs.set("vertexLabel", nodeMapper.label());
			//TODO:: add the statement to cache
			return sgs;
		}
		
		//if node is an edge
		if(nodeMapper instanceof EdgeMapper<?,?,?>){
			EdgeMapper<T,S,E> edgeMapper = (EdgeMapper<T,S,E>) nodeMapper;
			VertexMapper<S> fromVertex = edgeMapper.getFromVertex();
			VertexMapper<E> toVertex = edgeMapper.getToVertex();
			
			S fromObj = (S) edgeMapper.getFromProperty().getValue(node);
			E toObj = (E) edgeMapper.getToProperty().getValue(node);
			
			StringBuffer sb = new StringBuffer();
			sb.append("g.V().hasLabel(v1Label)");
			sb.append(DOT).append(HAS).append(LEFT_PARENTHESIS);
			for(PropertyMapper<S> prop: fromVertex.getIdProperties()){
				sb.append(quote(prop.getPropertyName())).append(COMMA).append(quote(String.valueOf(prop.getValue(fromObj))));
			}
			
			sb.append(RIGHT_PARENTHESIS);
			sb.append(DOT);
			sb.append("as('v1')");
			sb.append(DOT);
			sb.append("V().hasLabel(v2Label)");
			sb.append(DOT).append(HAS).append(LEFT_PARENTHESIS);
			
			for(PropertyMapper<E> prop: toVertex.getIdProperties()){
				sb.append(quote(prop.getPropertyName())).append(COMMA).append(quote(String.valueOf(prop.getValue(toObj))));
			}
			sb.append(RIGHT_PARENTHESIS);
			sb.append(DOT);
			sb.append("addE");
			sb.append(LEFT_PARENTHESIS);
			sb.append(quote(edgeMapper.label()));
			sb.append(RIGHT_PARENTHESIS);
			sb.append(DOT);
			sb.append("from('v1')");
			//edge properties
			for(Map.Entry<PropertyMapper<T>, Object> entry: propValMap.entrySet()){
				sb.append(DOT).append(PROPERTY)
				.append(LEFT_PARENTHESIS)
				.append(quote(entry.getKey().getPropertyName()))
				.append(COMMA)
				.append(quote(entry.getValue().toString()))
				.append(RIGHT_PARENTHESIS);
			}
			SimpleGraphStatement sgs = new SimpleGraphStatement(sb.toString());
			sgs.set("v1Label", fromVertex.label());
			sgs.set("v2Label", toVertex.label());
			
			return sgs;
		}
		
		return null;
	}
	
	private String quote(String s){
		return "'"+s +"'";
	}
	
	@SuppressWarnings("unchecked")
	public <E, S> NodeResult<T> map(GraphResultSet grs){
		if(nodeMapper instanceof VertexMapper<?>){
			return new VertexResult<T>(grs,(VertexMapper<T>)nodeMapper);
		}else if(nodeMapper instanceof EdgeMapper<?,?,?>){
			return new EdgeResult<T,S,E>(grs, (EdgeMapper<T,S,E>)nodeMapper);
		}
		return null;
	}
	
	//TODO:: come up with unique key generation algorithm for statement cache
	private static class QueryKey{
		
		private String queryType;
		
		QueryKey(String queryType) {
			super();
			this.queryType = queryType;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((queryType == null) ? 0 : queryType.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			QueryKey other = (QueryKey) obj;
			if (queryType == null) {
				if (other.queryType != null)
					return false;
			} else if (!queryType.equals(other.queryType))
				return false;
			return true;
		}
	}
	public GraphMappingManager getMappingManager() {
		return mappingManager;
	}


	public NodeMapper<T> getNodeMapper() {
		return nodeMapper;
	}
	
	
}
