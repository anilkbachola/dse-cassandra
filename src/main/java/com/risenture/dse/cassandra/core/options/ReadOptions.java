package com.risenture.dse.cassandra.core.options;

import com.datastax.driver.core.PagingState;

public class ReadOptions extends StatementOptions {
  private int fetchSize;
  private PagingState pagingState;

  public int getFetchSize() {
    return fetchSize;
  }
  
  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }
  
  public PagingState getPagingState() {
    return pagingState;
  }
  
  public void setPagingState(PagingState pagingState) {
    this.pagingState = pagingState;
  }

}
