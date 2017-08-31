package com.risenture.dse.cassandra.core.options;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;

public class StatementOptions {
  private ConsistencyLevel consistency;
  private RetryPolicy retryPolicy;
  private Integer readTimeoutMillis;
  private ConsistencyLevel serialConsistency;
  private boolean idempotent;
  private long timestamp;
  private boolean tracing;

  public StatementOptions() {
    super();
  }

  public ConsistencyLevel getConsistency() {
    return consistency;
  }
  
  public void setConsistency(ConsistencyLevel consistency) {
    this.consistency = consistency;
  }
  
  public StatementOptions withConsistency(ConsistencyLevel consistency) {
    this.consistency = consistency;
    return this;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }
  
  public void setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }
  
  public StatementOptions withRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  public Integer getReadTimeoutMillis() {
    return readTimeoutMillis;
  }
  
  public void setReadTimeoutMillis(Integer readTimeoutMillis) {
    this.readTimeoutMillis = readTimeoutMillis;
  }
  
  public StatementOptions withReadTimeoutMillis(Integer readTimeoutMillis) {
    this.readTimeoutMillis = readTimeoutMillis;
    return this;
  }

  public ConsistencyLevel getSerialConsistency() {
    return serialConsistency;
  }
  
  public void setSerialConsistency(ConsistencyLevel serialConsistency) {
    if (serialConsistency != ConsistencyLevel.SERIAL 
        && serialConsistency != ConsistencyLevel.LOCAL_SERIAL) {
      throw new IllegalArgumentException();
    }
    this.serialConsistency = serialConsistency;
  }
  
  public StatementOptions withSerialConsistency(ConsistencyLevel serialConsistency) {
    if (serialConsistency != ConsistencyLevel.SERIAL 
        && serialConsistency != ConsistencyLevel.LOCAL_SERIAL) {
      throw new IllegalArgumentException();
    }
    this.serialConsistency = serialConsistency;
    return this;
  }

  public boolean isIdempotent() {
    return idempotent;
  }
  
  public void setIdempotent(boolean idempotent) {
    this.idempotent = idempotent;
  }
  
  public StatementOptions withIdempotent(boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  public long getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public StatementOptions withTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public boolean isTracing() {
    return tracing;
  }
  
  public void setTracing(boolean tracing) {
    this.tracing = tracing;
  }
  
  public StatementOptions withTracing(boolean tracing) {
    this.tracing = tracing;
    return this;
  }
  
}
