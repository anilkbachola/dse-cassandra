package com.risenture.dse.graph.data;

import java.util.List;

import com.datastax.driver.core.ExecutionInfo;

public abstract class NodeResult <T> implements Iterable<T>{

	public abstract List<T> all();
	
	public abstract T one();
	
	public abstract ExecutionInfo getExecutionInfo();
}
