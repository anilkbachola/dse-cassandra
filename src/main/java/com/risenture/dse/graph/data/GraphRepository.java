package com.risenture.dse.graph.data;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphNode;
import com.datastax.driver.dse.graph.GraphResultSet;
import com.datastax.driver.dse.graph.GraphStatement;
import com.datastax.driver.dse.graph.Vertex;
import com.datastax.driver.dse.graph.VertexProperty;
import com.risenture.dse.core.DseContext;
import com.risenture.dse.graph.GraphMapper;

public class GraphRepository<T> {
	
	private final GraphMapper<T> mapper;
	private Class<T> inferedClass;
	private final DseSession session;
	
	public GraphRepository(DseContext context){
		mapper = context.getMappingManager().mapper(getGenericClass());
		this.session = context.getSession();
	}

	@SuppressWarnings("unchecked")
	public Class<T> getGenericClass() {
		if (inferedClass == null) {
			Type mySuperclass = getClass().getGenericSuperclass();
			Type tType = ((ParameterizedType) mySuperclass).getActualTypeArguments()[0];
			String className = tType.toString().split(" ")[1];
			try {
				inferedClass = (Class<T>) Class.forName(className);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return inferedClass;
	}

	public void addVertex(T vertex){
		GraphStatement st = mapper.createQuery(vertex);
		session.executeGraph(st);
	}
	
	public void addEdge(T edge){
		GraphStatement st = mapper.createQuery(edge);
		session.executeGraph(st);
	}
	
	public NodeResult<T> list(String query){
		
		GraphResultSet grs = session.executeGraph(query);
		return mapper.map(grs);
		//GraphResult<T> gr = mapper.map(grs);
	}
	
}
