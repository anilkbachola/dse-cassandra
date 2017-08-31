package com.risenture.dse.graph;

import java.util.ArrayList;
import java.util.List;

public class VertexMapper<T> extends NodeMapper<T>{

	final List<PropertyMapper<T>> idProperties = new ArrayList<PropertyMapper<T>>();
	
	public VertexMapper(Class<T> entityClass, String label) {
		super(entityClass, label);
	}
	
	public void addProperties(List<PropertyMapper<T>> idProperties, List<PropertyMapper<T>> properties) {
		this.idProperties.addAll(idProperties);
		this.properties.addAll(idProperties);
		
		this.properties.addAll(properties);
	}

	public List<PropertyMapper<T>> getIdProperties() {
		return idProperties;
	}
	
}
