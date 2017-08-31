package com.risenture.dse.graph;


public class EdgeMapper<T, S, E> extends NodeMapper<T>{

	private final VertexMapper<S> fromVertex;
	private final VertexMapper<E> toVertex;
	private final PropertyMapper<T> fromProperty;
	private final PropertyMapper<T> toProperty;
	
	public EdgeMapper(Class<T> klass, String label,VertexMapper<S> fromVertex, VertexMapper<E> toVertex, 
			PropertyMapper<T> fromProperty, PropertyMapper<T> toProperty) {
		super(klass, label);
		
		if(fromVertex == null || toVertex == null)
			throw new IllegalArgumentException("Edge Definition requires 'from' and 'to' connection definitions");
		
		this.fromVertex = fromVertex;
		this.toVertex = toVertex;
		this.fromProperty = fromProperty;
		this.toProperty = toProperty;
	}

	public VertexMapper<S> getFromVertex() {
		return fromVertex;
	}

	public VertexMapper<E> getToVertex() {
		return toVertex;
	}

	public PropertyMapper<T> getFromProperty() {
		return fromProperty;
	}

	public PropertyMapper<T> getToProperty() {
		return toProperty;
	}
	
}
