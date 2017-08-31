package com.risenture.dse.graph;

import java.util.ArrayList;
import java.util.List;

public abstract class NodeMapper<T> {
	final Class<T> klass;
	final String label;

	final List<PropertyMapper<T>> properties = new ArrayList<PropertyMapper<T>>();

	public NodeMapper(Class<T> klass, String label) {
		super();
		if(label == null || "".equals(label))
			throw new IllegalArgumentException("Edge or Vertex must have label property defined");

		this.klass = klass;
		this.label = label;
	}

	public void addProperties(List<PropertyMapper<T>> properties) {
		this.properties.addAll(properties);
	}

	public List<PropertyMapper<T>> allProperties(){
		return this.properties;
	}

	public int numProperties(){
		return this.properties.size();
	}

	public String label(){
		return this.label;
	}

	public T newEntity() {
		try {
			return klass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Can't create an instance of " + klass.getName());
		}
	}

}
